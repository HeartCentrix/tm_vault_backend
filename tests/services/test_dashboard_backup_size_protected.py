"""Protected-data display and captured-volume estimates have separate contracts."""
import importlib.util
from pathlib import Path
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import pytest


@pytest.fixture
def ds():
    spec = importlib.util.spec_from_file_location('dashboard_size_response', Path(__file__).resolve().parents[2] / 'services/dashboard-service/main.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def baseline(**changes):
    return {'totalBytes': 1000, 'partial': False, 'stale': False,
            'initializingScope': False, 'freshness': 'fresh'} | changes


def test_projection_uses_complete_baseline_and_mean_of_thirty_days(ds):
    response = ds._size_response(baseline(), [{'date': str(i), 'bytes': 30} for i in range(30)], history_complete=True)
    assert response['totalBytes'] == 1000
    assert response['dailyGrowthBytes'] == 30
    assert response['projectedBytes1y'] == 1000 + 30 * 365
    assert response['projectedBytes3y'] == 1000 + 30 * 1095
    assert response['growthMetric'] == 'captured-change-volume'
    assert response['oneMonthChange'] == response['sevenDayChange']


def test_unavailable_history_preserves_good_total_but_not_estimates(ds):
    response = ds._size_response(baseline(), [], history_complete=False)
    assert response['totalBytes'] == 1000
    assert response['partial'] is False
    assert response['historyPartial'] is True
    assert response['projectedBytes1y'] is None
    assert response['dailyGrowthBytes'] is None
    assert response['sevenDayChange'] is None


def test_missing_baseline_is_calculating_not_zero(ds):
    response = ds._size_response(baseline(totalBytes=None, partial=True, freshness='building'), [], history_complete=False)
    assert response['total'] == 'Calculating'
    assert response['totalBytes'] is None
    assert response['allTimeTotal'] is None
    assert response['projectedBytes3y'] is None
