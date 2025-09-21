# Architecture & Design Patterns Documentation

## Overview

This data generation system is built using multiple design patterns to create a flexible, maintainable, and extensible architecture. The system can generate realistic synthetic data for any JSON schema while maintaining DWP safety standards.

## Design Patterns Used

### 1. **Strategy Pattern**
**Purpose**: Different data generation algorithms
**Location**: `generators/concrete_generators.py`

```python
# Each generator implements a specific strategy
class StringGenerator(DataGenerator):
    def generate(self, constraints: Dict) -> str:
        # String generation strategy

class NumberGenerator(DataGenerator):
    def generate(self, constraints: Dict) -> float:
        # Number generation strategy
```

**Benefits**:
- Easy to add new generation strategies
- Algorithms can be swapped at runtime
- Each strategy is isolated and testable

### 2. **Abstract Factory Pattern**
**Purpose**: Create families of related generators
**Location**: `generators/generator_factory.py`

```python
class GeneratorFactory(ABC):
    @abstractmethod
    def create_string_generator(self) -> DataGenerator:
        pass

    @abstractmethod
    def create_number_generator(self) -> DataGenerator:
        pass

# Concrete factories for different domains
class StandardGeneratorFactory(GeneratorFactory):
    # Creates standard generators

class DWPGeneratorFactory(GeneratorFactory):
    # Creates DWP-specific generators
```

**Benefits**:
- Ensures related generators work together
- Easy to create domain-specific generator families
- Centralizes generator creation logic

### 3. **Factory Method Pattern**
**Purpose**: Create objects based on type
**Location**: `generators/generator_factory.py`

```python
def create_specialized_generator(self, generator_type: str) -> DataGenerator:
    generators = {
        "contact": ContactGenerator(),
        "boolean": BooleanGenerator(),
        "uuid": UUIDGenerator(),
    }
    return generators[generator_type]
```

**Benefits**:
- Encapsulates object creation
- Easy to extend with new types
- Centralized type mapping

### 4. **Registry Pattern**
**Purpose**: Maintain registry of available factories
**Location**: `generators/generator_factory.py`

```python
class GeneratorRegistry:
    _instance = None  # Singleton
    _registry = {}

    def get_factory(self, factory_type: str) -> GeneratorFactory:
        return self._registry[factory_type]
```

**Benefits**:
- Central registry of all factories
- Runtime factory registration
- Decouples factory selection from usage

### 5. **Singleton Pattern**
**Purpose**: Ensure single registry instance
**Location**: `generators/generator_factory.py`

```python
def __new__(cls):
    if cls._instance is None:
        cls._instance = super(GeneratorRegistry, cls).__new__(cls)
    return cls._instance
```

**Benefits**:
- Global access point to registry
- Prevents multiple registry instances
- Consistent state across application

### 6. **Chain of Responsibility Pattern**
**Purpose**: Process schema fields through sequential handlers
**Location**: `schema_processor.py`

```python
class SchemaProcessor(ABC):
    def set_next(self, processor: 'SchemaProcessor'):
        self._next_processor = processor

    def process(self, context: ProcessingContext):
        self._handle_processing(context)
        if self._next_processor:
            return self._next_processor.process(context)

# Chain: TypeProcessor -> ConstraintProcessor -> SemanticProcessor -> BusinessRuleProcessor
```

**Benefits**:
- Flexible processing pipeline
- Easy to add/remove processing steps
- Each processor has single responsibility

### 7. **Template Method Pattern**
**Purpose**: Define algorithm skeleton with customizable steps
**Location**: `schema_data_generator.py`, `generators/concrete_generators.py`

```python
def generate_from_schema(self, schema: Dict, num_records: int) -> List[Dict]:
    # Template method defines the overall process
    properties = schema.get("properties", {})
    required_fields = schema.get("required", [])

    records = []
    for i in range(num_records):
        record = self._generate_single_record(properties, required_fields)  # Customizable step
        records.append(record)
    return records
```

**Benefits**:
- Consistent algorithm structure
- Customizable behavior at specific points
- Code reuse across similar processes

### 8. **Facade Pattern**
**Purpose**: Simple interface to complex subsystem
**Location**: `schema_data_generator.py`

```python
class SchemaDataGenerator:
    """Facade - provides simple interface to complex generation system"""

    def generate_from_schema(self, schema: Dict, num_records: int) -> List[Dict]:
        # Hides complexity of registry, factories, processors, etc.
        # Simple method call handles entire generation process
```

**Benefits**:
- Simplified API for users
- Hides internal complexity
- Decouples clients from subsystem

### 9. **Builder Pattern**
**Purpose**: Construct complex processing chain
**Location**: `schema_processor.py`

```python
def _build_chain(self) -> SchemaProcessor:
    """Builder - constructs complex object step by step"""
    type_processor = TypeProcessor()
    constraint_processor = ConstraintProcessor()
    semantic_processor = SemanticProcessor()
    business_rule_processor = BusinessRuleProcessor()

    # Build the chain step by step
    type_processor.set_next(constraint_processor) \\
                 .set_next(semantic_processor) \\
                 .set_next(business_rule_processor)
    return type_processor
```

**Benefits**:
- Step-by-step construction of complex objects
- Fluent interface for configuration
- Separation of construction from representation

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SchemaDataGenerator                      │
│                     (Facade Pattern)                       │
├─────────────────────────────────────────────────────────────┤
│ • Simple interface for users                               │
│ • Orchestrates entire generation process                   │
│ • Hides internal complexity                                │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│              GeneratorRegistry                              │
│               (Singleton Pattern)                          │
├─────────────────────────────────────────────────────────────┤
│ • Single instance managing all factories                   │
│ • Runtime factory registration                             │
│ • Decouples factory selection                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│            GeneratorFactory                                 │
│         (Abstract Factory Pattern)                         │
├─────────────────────────────────────────────────────────────┤
│ StandardGeneratorFactory  │  DWPGeneratorFactory           │
│ • Creates standard        │  • Creates DWP-specific        │
│   generators              │    generators                  │
│ • General purpose         │  • Domain-specific rules       │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│             DataGenerators                                  │
│            (Strategy Pattern)                               │
├─────────────────────────────────────────────────────────────┤
│ StringGenerator    │ NumberGenerator    │ DateGenerator     │
│ ContactGenerator   │ BooleanGenerator   │ UUIDGenerator     │
│                                                             │
│ • Each implements specific generation strategy              │
│ • Interchangeable algorithms                               │
│ • Isolated and testable                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│          SchemaProcessorChain                               │
│       (Chain of Responsibility)                             │
├─────────────────────────────────────────────────────────────┤
│ TypeProcessor → ConstraintProcessor → SemanticProcessor     │
│                              ↓                             │
│                    BusinessRuleProcessor                    │
│                                                             │
│ • Sequential processing of schema fields                   │
│ • Each processor handles specific concern                  │
│ • Flexible and extensible pipeline                         │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **User Input**: User provides JSON schema + record count
2. **Facade**: `SchemaDataGenerator` receives request
3. **Registry**: Gets appropriate factory from singleton registry
4. **Schema Processing**: Each field processed through chain:
   - `TypeProcessor`: Maps types to generators
   - `ConstraintProcessor`: Validates and processes constraints
   - `SemanticProcessor`: Infers meaning from field names
   - `BusinessRuleProcessor`: Applies domain-specific rules
5. **Generator Selection**: Factory creates appropriate generator
6. **Value Generation**: Strategy pattern generates values
7. **Record Assembly**: Template method assembles complete records
8. **Output**: Returns generated data + optional JSON-LD conversion

## DWP Safety Features

### NINO Generation Safety
- **Invalid Prefixes**: Uses only test-safe prefixes (BG, GB, NK, KN, TN, NT, ZZ, etc.)
- **Pattern Validation**: Ensures correct format but invalid for real use
- **Business Rules**: Automatic detection and safe generation

### Validation Chain
```
Field Name → Semantic Recognition → NINO Detection → Test-Only Generation
    ↓              ↓                    ↓               ↓
"nino"  →  Contains "nino" → Apply NINO rules → Generate TN123456A
```

## Extensibility Points

### Adding New Generators
```python
class CustomGenerator(DataGenerator):
    def generate(self, constraints: Dict) -> Any:
        # Custom generation logic

    def validate_constraints(self, constraints: Dict) -> bool:
        # Custom validation
```

### Adding New Processors
```python
class CustomProcessor(SchemaProcessor):
    def _handle_processing(self, context: ProcessingContext):
        # Custom processing logic
```

### Adding New Factories
```python
class CustomFactory(GeneratorFactory):
    def create_string_generator(self) -> DataGenerator:
        return CustomStringGenerator()
```

## Performance Optimizations

1. **Generator Caching**: Generators cached to avoid recreation
2. **Singleton Registry**: Single instance prevents overhead
3. **Lazy Loading**: Generators created only when needed
4. **Strategy Selection**: Efficient type-to-generator mapping

## Benefits of This Architecture

1. **Maintainability**: Clear separation of concerns
2. **Extensibility**: Easy to add new generators/processors
3. **Testability**: Each component is isolated and testable
4. **Flexibility**: Runtime configuration and strategy selection
5. **Reusability**: Components can be reused across domains
6. **Safety**: Built-in DWP compliance and validation
7. **Performance**: Optimized for high-volume generation

This architecture ensures the system remains flexible while maintaining safety standards and high performance for large-scale data generation.