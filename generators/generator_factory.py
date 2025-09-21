"""
Generator Factory - Abstract Factory Pattern Implementation
Creates families of related data generators
"""

from typing import Dict
from .base_generator import GeneratorFactory, DataGenerator
from .concrete_generators import (
    StringGenerator,
    NumberGenerator,
    DateGenerator,
    ContactGenerator,
    BooleanGenerator,
    UUIDGenerator
)


class StandardGeneratorFactory(GeneratorFactory):
    """
    Concrete Factory - Creates standard data generators
    Implements Abstract Factory Pattern
    """

    def create_string_generator(self) -> DataGenerator:
        """Create string generator"""
        return StringGenerator()

    def create_number_generator(self) -> DataGenerator:
        """Create number generator"""
        return NumberGenerator()

    def create_date_generator(self) -> DataGenerator:
        """Create date generator"""
        return DateGenerator()

    def create_specialized_generator(self, generator_type: str) -> DataGenerator:
        """
        Create specialized generators based on type
        Factory Method Pattern - creates objects based on type
        """
        generators = {
            "contact": ContactGenerator(),
            "boolean": BooleanGenerator(),
            "uuid": UUIDGenerator(),
            "email": ContactGenerator(),
            "phone": ContactGenerator(),
            "url": ContactGenerator(),
        }

        if generator_type not in generators:
            raise ValueError(f"Unknown generator type: {generator_type}")

        return generators[generator_type]


class DWPGeneratorFactory(GeneratorFactory):
    """
    Specialized factory for DWP-specific generators
    Extends the base factory to provide DWP-specific implementations
    """

    def __init__(self):
        self.standard_factory = StandardGeneratorFactory()

    def create_string_generator(self) -> DataGenerator:
        """Create DWP-specific string generator"""
        return self.standard_factory.create_string_generator()

    def create_number_generator(self) -> DataGenerator:
        """Create DWP-specific number generator"""
        return self.standard_factory.create_number_generator()

    def create_date_generator(self) -> DataGenerator:
        """Create DWP-specific date generator"""
        return self.standard_factory.create_date_generator()

    def create_specialized_generator(self, generator_type: str) -> DataGenerator:
        """Create DWP-specific specialized generators"""
        if generator_type == "nino":
            # Return string generator configured for NINO generation
            return self.standard_factory.create_string_generator()
        else:
            return self.standard_factory.create_specialized_generator(generator_type)


class GeneratorRegistry:
    """
    Registry Pattern - Maintains a registry of available generators
    Singleton Pattern - ensures only one registry exists
    """

    _instance = None
    _registry = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GeneratorRegistry, cls).__new__(cls)
            cls._instance._initialize_registry()
        return cls._instance

    def _initialize_registry(self):
        """Initialize the generator registry"""
        self._registry = {
            "standard": StandardGeneratorFactory(),
            "dwp": DWPGeneratorFactory()
        }

    def get_factory(self, factory_type: str = "standard") -> GeneratorFactory:
        """Get factory by type"""
        if factory_type not in self._registry:
            raise ValueError(f"Unknown factory type: {factory_type}")
        return self._registry[factory_type]

    def register_factory(self, name: str, factory: GeneratorFactory):
        """Register a new factory"""
        self._registry[name] = factory

    def list_factories(self) -> list:
        """List all registered factories"""
        return list(self._registry.keys())