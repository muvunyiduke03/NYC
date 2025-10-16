import logging
import math
from typing import Tuple, Any, List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SpatialGridIndex:

    def __init__(self, grid_size: float = 0.01):
        self.grid_size = grid_size
        self.grid = {}
        logger.info(f"Initialized SpatialGridIndex with grid_size={grid_size}")

    def _get_cell_key(self, lat: float, lon: float) -> Tuple[int, int]:
        cell_x = int(math.floor(lon / self.grid_size))
        cell_y = int(math.floor(lat / self.grid_size))
        return cell_x, cell_y

    def insert(self, lat: float, lon: float, data: Any):
        cell_key = self._get_cell_key(lat, lon)
        if cell_key not in self.grid:
            self.grid[cell_key] = []
        self.grid[cell_key].append(data)

    def get_statistics(self) -> Dict[str, Any]:
        cell_counts = [len(points) for points in self.grid.values()]
        return {
            'total_cells': len(self.grid),
            'total_points': sum(cell_counts),
            'avg_points_per_cell': sum(cell_counts) / len(cell_counts) if cell_counts else 0,
            'max_points_in_cell': max(cell_counts) if cell_counts else 0
        }

    def get_cell_bounds(self, cell_key: Tuple[int, int]) -> Tuple[float, float, float, float]:
        cell_x, cell_y = cell_key
        min_lon = cell_x * self.grid_size
        max_lon = (cell_x + 1) * self.grid_size
        min_lat = cell_y * self.grid_size
        max_lat = (cell_y + 1) * self.grid_size
        return min_lat, max_lat, min_lon, max_lon