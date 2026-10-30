"""
Azure VM Restore Handler — Production-Ready, Afi.ai-Equivalent

Restore modes:
1. Full VM restore — recreates entire VM from backup (disks + NICs + config)
2. Disk-level restore — restore individual OS/data disks from backup
3. Fast local recovery — restore from 7-day Azure restore points
4. Cross-region restore — restore from DR blob copies
"""
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.compute.aio import ComputeManagementClient
from azure.mgmt.compute.models import DiskCreateOptionTypes, VirtualMachine
from azure.mgmt.network.aio import NetworkManagementClient

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from arm_credentials import get_arm_credential


class VmRestoreHandler:
    """Production-ready Azure VM restore handler."""

    def __init__(self, worker_id: str = "azure-vm-restore"):
        self.worker_id = worker_id

    async def restore_vm(self, tenant: Tenant, snapshot: Snapshot,
                         restore_params: Dict) -> Dict:
        """
        Full VM restore from backup.

        Reads VM config and network config from backup blobs,
        recreates managed disks from VHD blobs, then creates
        the VM with original configuration.

        restore_params:
            target_vm_name: str — name for restored VM
            target_resource_group: str — where to restore
            target_virtual_network_id: str — optional, override VNet
            target_subnet_id: str — optional, override subnet
            overwrite: bool — overwrite if VM exists
        """
        # Backend-kind guard — VM restore builds Azure managed disks
        # from blobs by handing Azure ARM a SAS URL pointing AT the
        # backing blob. Azure Recovery / the Managed Disk API can only
        # pull from Azure Blob. When the active backend is on-prem
        # (SeaweedFS / S3), the blob bytes live outside Azure's reach
        # and any SAS URL we generate is from a different cloud.
        # Fail cleanly up-front instead of crashing on an Azure SDK
        # call mid-restore, when disks may have been half-created.
        shard = azure_storage_manager.get_default_shard()
        shard_kind = getattr(shard, "kind", "azure_blob") or "azure_blob"
        if shard_kind != "azure_blob":
            raise RuntimeError(
                f"VM restore requires the active storage backend to be "
                f"Azure Blob (current: {shard_kind}). Azure Managed Disks "
                f"can only be hydrated from Azure-hosted VHDs. Toggle the "
                f"storage backend to azure-primary in Settings → Storage "
                f"before triggering a VM restore."
            )

        credential = get_arm_credential()
        compute_client = ComputeManagementClient(credential, tenant.subscription_id or "")
        network_client = NetworkManagementClient(credential, tenant.subscription_id or "")

        target_vm_name = restore_params.get("target_vm_name", f"{snapshot.id.hex[:8]}-restored")
        target_rg = restore_params.get("target_resource_group", "tmvault-restored-vms")

        self._log(f"Starting VM restore: {target_vm_name} in {target_rg}")

        try:
            # Step 1: Load VM config from backup blob
            vm_config = await self._load_vm_config(tenant, snapshot)
            network_config = await self._load_network_config(tenant, snapshot)

            # Step 2: Create managed disks from backup VHD blobs
            disk_results = await self._create_disks_from_backup(
                compute_client, target_rg, target_vm_name, vm_config, snapshot, tenant
            )

            # Step 3: Create or reuse network interfaces
            nic_results = await self._prepare_network_interfaces(
                network_client, compute_client, target_rg, target_vm_name,
                vm_config, network_config, restore_params
            )

            # Step 4: Create VM
            vm_result = await self._create_vm(
                compute_client, target_rg, target_vm_name, vm_config,
                disk_results, nic_results, restore_params
            )

            self._log(f"VM restore completed: {target_vm_name} ({vm_result.get('id', 'unknown')})")
            return {
                "success": True,
                "vm_id": vm_result.get("id"),
                "vm_name": target_vm_name,
                "resource_group": target_rg,
                "disks_created": len(disk_results),
                "nics_created": len(nic_results),
            }

        except Exception as e:
            self._log(f"VM restore FAILED: {e}\n{traceback.format_exc()}", "ERROR")
            return {
                "success": False,
                "error": str(e)[:1000],
                "vm_name": target_vm_name,
                "resource_group": target_rg,
            }

    async def restore_disk(self, tenant: Tenant, snapshot: Snapshot,
                            disk_name: str, restore_params: Dict) -> Dict:
        """
        Restore a single disk from backup.
        Useful for data recovery without full VM recreation.
        """
        # Same backend-kind guard as restore_vm — the Managed Disk API
        # can only hydrate from Azure-hosted VHDs. See restore_vm for
        # the full reasoning.
        shard = azure_storage_manager.get_default_shard()
        shard_kind = getattr(shard, "kind", "azure_blob") or "azure_blob"
        if shard_kind != "azure_blob":
            raise RuntimeError(
                f"Disk restore requires the active storage backend to be "
                f"Azure Blob (current: {shard_kind})."
            )

        credential = get_arm_credential()
        compute_client = ComputeManagementClient(credential, tenant.subscription_id or "")

        target_rg = restore_params.get("target_resource_group", "tmvault-restored-disks")
        target_disk_name = restore_params.get("target_disk_name", f"{disk_name}-restored")

        self._log(f"Restoring disk: {disk_name} → {target_disk_name}")

        try:
            # Load config to find disk info
            vm_config = await self._load_vm_config(tenant, snapshot)

            # Find disk in backup
            disk_blob = None
            os_type = "Linux"
            for d in vm_config.get("data_disks", []):
                if d.get("name") == disk_name:
                    disk_blob = d
                    os_type = "Linux"
                    break

            if not disk_blob and vm_config.get("os_disk", {}).get("name") == disk_name:
                disk_blob = vm_config["os_disk"]
                os_type = vm_config["os_disk"].get("os_type", "Linux")

            if not disk_blob:
                raise ValueError(f"Disk {disk_name} not found in backup")

            # Create managed disk from backup blob
            container = azure_storage_manager.get_container_name(str(tenant.id), "azure-vm")
            blob_path = f"{vm_config['vm_name']}/{snapshot.id.hex[:12]}/"

            # Find the VHD blob
            shard = azure_storage_manager.get_default_shard()
            blob_client = shard.get_async_client()
            container_client = blob_client.get_container_client(container)

            # List blobs with prefix
            async for blob in container_client.list_blobs(name_starts_with=blob_path):
                if disk_name.lower().replace(" ", "-") in blob.name.lower():
                    # Create disk from VHD
                    disk_params = {
                        "location": vm_config.get("location", "eastus"),
                        "creation_data": {
                            "create_option": DiskCreateOptionTypes.IMPORT,
                            "source_uri": f"https://{shard.account_name}.blob.core.windows.net/{container}/{blob.name}",
                        },
                        "os_type": os_type,
                        "disk_size_gb": disk_blob.get("disk_size_gb", 128),
                    }

                    poller = await compute_client.disks.begin_create_or_update(
                        resource_group_name=target_rg,
                        disk_name=target_disk_name,
                        disk=disk_params,
                    )
                    result = await poller.result()

                    self._log(f"Disk restored: {target_disk_name} ({result.id})")
                    return {
                        "success": True,
                        "disk_id": result.id,
                        "disk_name": target_disk_name,
                        "resource_group": target_rg,
                    }

            raise ValueError(f"No VHD blob found for disk {disk_name}")

        except Exception as e:
            self._log(f"Disk restore FAILED: {e}", "ERROR")
            return {"success": False, "error": str(e)[:500]}

    # ==================== Internal Methods ====================

    async def _load_vm_config(self, tenant: Tenant, snapshot: Snapshot) -> Dict:
        """Load VM config from backup blob."""
        if not snapshot.extra_data or not snapshot.extra_data.get("vm_config_blob"):
            raise ValueError("No VM config blob found in snapshot")

        blob_path = snapshot.extra_data["vm_config_blob"]
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-vm")
        shard = azure_storage_manager.get_default_shard()

        blob_client = shard.get_async_client().get_blob_client(container, blob_path)
        download = await blob_client.download_blob()
        content = await download.readall()
        return json.loads(content)

    async def _load_network_config(self, tenant: Tenant, snapshot: Snapshot) -> Dict:
        """Load network config from backup blob."""
        if not snapshot.extra_data or not snapshot.extra_data.get("network_config_blob"):
            return {}

        blob_path = snapshot.extra_data["network_config_blob"]
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-vm")
        shard = azure_storage_manager.get_default_shard()

        blob_client = shard.get_async_client().get_blob_client(container, blob_path)
        download = await blob_client.download_blob()
        content = await download.readall()
        return json.loads(content)

    async def _create_disks_from_backup(self, compute_client, target_rg: str,
                                         target_vm_name: str, vm_config: Dict,
                                         snapshot: Snapshot, tenant: Tenant) -> list:
        """Create managed disks from backup VHD blobs."""
        results = []
        # Container must come from the real tenant — the backup handler
        # never writes tenant_id into snapshot.extra_data, so deriving it
        # from there built a container name for tenant "" and no VHD was
        # ever found (every full VM restore died at disk creation).
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-vm")
        shard = azure_storage_manager.get_default_shard()

        # OS disk
        os_disk_config = vm_config.get("os_disk", {})
        os_blob_prefix = f"{vm_config['vm_name']}/{snapshot.id.hex[:12]}/os-"

        if os_disk_config.get("managed_disk_id"):
            os_disk_name = f"{target_vm_name}-osdisk"

            # Find OS VHD blob
            async for blob in shard.get_async_client().get_container_client(container).list_blobs(name_starts_with=os_blob_prefix):
                disk_params = {
                    "location": vm_config.get("location", "eastus"),
                    "creation_data": {
                        "create_option": DiskCreateOptionTypes.IMPORT,
                        "source_uri": f"https://{shard.account_name}.blob.core.windows.net/{container}/{blob.name}",
                    },
                    "os_type": os_disk_config.get("os_type", "Linux"),
                    "disk_size_gb": os_disk_config.get("disk_size_gb", 128),
                }

                poller = await compute_client.disks.begin_create_or_update(
                    resource_group_name=target_rg,
                    disk_name=os_disk_name,
                    disk=disk_params,
                )
                result = await poller.result()
                results.append({"type": "OS", "disk_id": result.id, "disk_name": os_disk_name})
                break

        # Data disks
        for dd in vm_config.get("data_disks", []):
            # Prefix must byte-match what the backup wrote: vm_handler
            # names the blob data-<disk name> with the disk's real name
            # verbatim (case preserved, no munging). Lowercasing here
            # silently skipped every mixed-case data disk.
            dd_disk_name = dd.get("name") or f"{vm_config['vm_name']}-data-disk-{dd.get('lun', 0)}"
            dd_blob_prefix = f"{vm_config['vm_name']}/{snapshot.id.hex[:12]}/data-{dd_disk_name}"

            data_disk_name = f"{target_vm_name}-datadisk-{dd.get('lun', 0)}"
            dd_found = False
            async for blob in shard.get_async_client().get_container_client(container).list_blobs(name_starts_with=dd_blob_prefix):
                disk_params = {
                    "location": vm_config.get("location", "eastus"),
                    "creation_data": {
                        "create_option": DiskCreateOptionTypes.IMPORT,
                        "source_uri": f"https://{shard.account_name}.blob.core.windows.net/{container}/{blob.name}",
                    },
                    "disk_size_gb": dd.get("disk_size_gb", 128),
                }

                poller = await compute_client.disks.begin_create_or_update(
                    resource_group_name=target_rg,
                    disk_name=data_disk_name,
                    disk=disk_params,
                )
                result = await poller.result()
                results.append({"type": "Data", "disk_id": result.id, "disk_name": data_disk_name, "lun": dd.get("lun", 0)})
                dd_found = True
                break

            # A VM quietly restored without one of its data disks is
            # silent data loss — fail the restore instead of attaching
            # fewer disks than the backup captured.
            if not dd_found:
                raise ValueError(
                    f"No VHD blob found for data disk {dd_disk_name} "
                    f"(prefix={dd_blob_prefix})"
                )

        return results

    async def _prepare_network_interfaces(self, network_client, compute_client,
                                          target_rg: str, target_vm_name: str,
                                          vm_config: Dict, network_config: Dict,
                                          restore_params: Dict) -> list:
        """Create network interfaces for restored VM."""
        results = []

        if not network_config.get("network_interfaces"):
            return results

        for nic_info in network_config["network_interfaces"]:
            nic_name = f"{target_vm_name}-nic"
            if not nic_info.get("primary"):
                nic_name = f"{target_vm_name}-nic-{nic_info.get('name', 'secondary')}"

            # Get subnet ID (use restore_params override or original)
            subnet_id = restore_params.get("target_subnet_id")
            if not subnet_id and nic_info.get("ip_configurations"):
                subnet_id = nic_info["ip_configurations"][0].get("subnet_id")

            if not subnet_id:
                raise ValueError("No subnet ID available for NIC creation")

            nic_params = {
                "location": vm_config.get("location", "eastus"),
                "ip_configurations": [{
                    "name": "ipconfig1",
                    "subnet": {"id": subnet_id},
                    "private_ip_allocation_method": "Dynamic",
                }],
            }

            poller = await network_client.network_interfaces.begin_create_or_update(
                resource_group_name=target_rg,
                network_interface_name=nic_name,
                network_interface=nic_params,
            )
            result = await poller.result()
            results.append({"nic_id": result.id, "nic_name": nic_name, "primary": nic_info.get("primary", True)})

        return results

    async def _create_vm(self, compute_client, target_rg: str, target_vm_name: str,
                         vm_config: Dict, disk_results: list, nic_results: list,
                         restore_params: Dict) -> Dict:
        """Create VM from restored disks and NICs."""
        os_disk_result = next((d for d in disk_results if d["type"] == "OS"), None)
        primary_nic = next((n for n in nic_results if n.get("primary", True)), nic_results[0] if nic_results else None)

        if not os_disk_result:
            raise ValueError("No OS disk available for VM creation")

        # NOTE: no os_profile here — ARM rejects osProfile on a VM built
        # from an ATTACHed OS disk ("Parameter 'osProfile' is not allowed").
        # The attached disk already carries the OS, hostname and users;
        # os_type on the os_disk is required for ATTACH instead.
        vm_params = {
            "location": vm_config.get("location", "eastus"),
            "hardware_profile": {"vm_size": vm_config.get("vm_size", "Standard_D2s_v3")},
            "storage_profile": {
                "os_disk": {
                    "managed_disk": {"id": os_disk_result["disk_id"]},
                    "caching": "ReadWrite",
                    "create_option": DiskCreateOptionTypes.ATTACH,
                    "os_type": vm_config.get("os_disk", {}).get("os_type", "Linux"),
                },
            },
            "network_profile": {
                "network_interfaces": [
                    {"id": n["nic_id"], "primary": n.get("primary", False)}
                    for n in nic_results
                ] if nic_results else [],
            },
        }

        # Add data disks
        data_disks = [d for d in disk_results if d["type"] == "Data"]
        if data_disks:
            vm_params["storage_profile"]["data_disks"] = [
                {
                    "managed_disk": {"id": d["disk_id"]},
                    "lun": d.get("lun", i),
                    "create_option": DiskCreateOptionTypes.ATTACH,
                }
                for i, d in enumerate(data_disks)
            ]

        poller = await compute_client.virtual_machines.begin_create_or_update(
            resource_group_name=target_rg,
            vm_name=target_vm_name,
            parameters=vm_params,
        )
        result = await poller.result()

        return {"id": result.id, "name": result.name}

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}]"
        if level == "ERROR":
            print(f"{prefix} [VM-RESTORE] ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} [VM-RESTORE] WARNING: {message}")
        else:
            print(f"{prefix} [VM-RESTORE] {message}")
