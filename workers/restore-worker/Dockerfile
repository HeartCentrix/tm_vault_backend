# --- Stage 1: build the vendored pstwriter CLI ----------------------------
# Compiles the standalone C++17 PST writer (vendored from `PST Dev`) and
# produces the `pst_convert` binary the worker shells out to. Aspose.Email
# is no longer used; the .NET runtime stage was removed with this change.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS pstwriter-build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        g++ \
        ninja-build \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY vendor/pstwriter /src

# Wipe any stray build/ that snuck through .dockerignore — without
# this, cmake reuses host-built artefacts (Mach-O on macOS dev
# machines) and the runtime stage ships a wrong-architecture binary
# that errors with `Exec format error`. Belt + suspenders alongside
# the .dockerignore entry.
RUN rm -rf /src/build /src/CMakeCache.txt /src/CMakeFiles \
 && cmake -S /src -B /src/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
 && cmake --build /src/build --target pst_convert \
 && /src/build/bin/pst_convert 2>/dev/null || true   # smoke: prints usage on argc!=4

# Sanity-check the produced binary is an ELF for the build platform.
# Magic bytes 7f 45 4c 46 ("ELF") — anything else (Mach-O cf fa ed fe,
# PE 4d 5a) means a wrong-arch artefact slipped through and the build
# should fail loudly here, not silently at runtime.
RUN head -c 4 /src/build/bin/pst_convert | od -An -tx1 | grep -q "7f 45 4c 46" \
 || (echo "FATAL: pst_convert is not an ELF binary. Build context likely contains a host-built artefact." \
     && head -c 4 /src/build/bin/pst_convert | od -An -tx1 \
     && exit 1)

# --- Stage 2: Python runtime ----------------------------------------------
FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

# Runtime libraries the pst_convert ELF needs (libstdc++, libgcc_s).
# python:3.11-slim already ships them; this RUN is a no-op safety net.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Drop the pst_convert binary from the build stage onto PATH.
COPY --from=pstwriter-build /src/build/bin/pst_convert /usr/local/bin/pst_convert
RUN chmod +x /usr/local/bin/pst_convert \
 && /usr/local/bin/pst_convert 2>/dev/null || true   # confirm it loads

COPY shared /app/shared
COPY workers/restore-worker /app/workers/restore-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir \
    -r /app/workers/restore-worker/requirements.lock

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Path to the bundled CLI; pstwriter_cli.py honours this override.
ENV PSTWRITER_CLI=/usr/local/bin/pst_convert

CMD ["python", "workers/restore-worker/main.py"]
