from typing import List
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class QuickSelect:
    """
    QuickSelect algorithm for finding Kth the smallest element.
    Used for calculating percentiles.
    Time Complexity: O(n) average case, O(n²) worst case
    """

    @staticmethod
    def partition(arr: List[float], low: int, high: int) -> int:
        """Partition array around pivot element"""
        pivot = arr[high]
        i = low - 1

        for j in range(low, high):
            if arr[j] <= pivot:
                i += 1
                arr[i], arr[j] = arr[j], arr[i]

        arr[i + 1], arr[high] = arr[high], arr[i + 1]
        return i + 1

    @classmethod
    def select(cls, arr: List[float], low: int, high: int, k: int) -> float:
        """Find kth smallest element using QuickSelect"""
        if low == high:
            return arr[low]

        pivot_index = cls.partition(arr, low, high)

        if k == pivot_index:
            return arr[k]
        elif k < pivot_index:
            return cls.select(arr, low, pivot_index - 1, k)
        else:
            return cls.select(arr, pivot_index + 1, high, k)

    @classmethod
    def find_percentile(cls, data: List[float], percentile: float) -> float:
        """Calculate percentile using QuickSelect"""
        if not data:
            return 0.0

        arr = data.copy()
        n = len(arr)
        k = int(percentile * n)
        k = min(k, n - 1)

        return cls.select(arr, 0, n - 1, k)