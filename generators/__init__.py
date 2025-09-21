"""
Generators Package - Design Pattern Implementations

This package implements several design patterns:
1. Strategy Pattern - Different data generation strategies
2. Abstract Factory Pattern - Creates families of related generators
3. Factory Method Pattern - Creates objects based on type
4. Registry Pattern - Maintains registry of available generators
5. Singleton Pattern - Ensures single registry instance
6. Template Method Pattern - Defines skeleton for pattern-based generation
"""

from .base_generator import DataGenerator, GeneratorFactory
from .concrete_generators import (
    StringGenerator,
    NumberGenerator,
    DateGenerator,
    ContactGenerator,
    BooleanGenerator,
    UUIDGenerator
)
from .generator_factory import (
    StandardGeneratorFactory,
    DWPGeneratorFactory,
    GeneratorRegistry
)

__all__ = [
    'DataGenerator',
    'GeneratorFactory',
    'StringGenerator',
    'NumberGenerator',
    'DateGenerator',
    'ContactGenerator',
    'BooleanGenerator',
    'UUIDGenerator',
    'StandardGeneratorFactory',
    'DWPGeneratorFactory',
    'GeneratorRegistry'
]