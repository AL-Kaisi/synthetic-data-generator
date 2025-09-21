"""
Base Generator - Abstract Factory Pattern
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class DataGenerator(ABC):
    """
    Abstract base class for all data generators.
    Implements the Strategy Pattern - defines interface for data generation strategies.
    """

    @abstractmethod
    def generate(self, constraints: Dict = None) -> Any:
        """Generate data based on constraints"""
        pass

    @abstractmethod
    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate that constraints are appropriate for this generator"""
        pass


class GeneratorFactory(ABC):
    """
    Abstract Factory Pattern - creates families of related generators
    """

    @abstractmethod
    def create_string_generator(self) -> DataGenerator:
        pass

    @abstractmethod
    def create_number_generator(self) -> DataGenerator:
        pass

    @abstractmethod
    def create_date_generator(self) -> DataGenerator:
        pass

    @abstractmethod
    def create_specialized_generator(self, generator_type: str) -> DataGenerator:
        pass