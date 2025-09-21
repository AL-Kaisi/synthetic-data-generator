# Design Pattern Visual Diagrams

## Complete System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                CLIENT CODE                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│ generator = SchemaDataGenerator(domain="dwp")                                   │
│ data = generator.generate_from_schema(schema, 1000)                             │
│ json_ld = generator.to_json_ld(data, schema)                                    │
└─────────────────────────┬───────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          FACADE PATTERN                                        │
│                      SchemaDataGenerator                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│ + __init__(domain: str)                                                         │
│ + generate_from_schema(schema, count) → List[Dict]                              │
│ + generate_field_value(name, schema) → Any                                      │
│ + to_json_ld(data, schema) → Dict                                               │
│                                                                                 │
│ - generator_registry: GeneratorRegistry                                        │
│ - factory: GeneratorFactory                                                     │
│ - schema_processor: SchemaProcessorChain                                        │
│ - _generator_cache: Dict[str, DataGenerator]                                    │
└─────────────────┬───────────────────┬───────────────────┬─────────────────────────┘
                  │                   │                   │
                  ▼                   ▼                   ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│   SINGLETON         │ │ ABSTRACT FACTORY    │ │ CHAIN OF            │
│   PATTERN           │ │ PATTERN             │ │ RESPONSIBILITY      │
│                     │ │                     │ │ PATTERN             │
│ GeneratorRegistry   │ │ GeneratorFactory    │ │ SchemaProcessorChain│
│                     │ │                     │ │                     │
│ ┌─────────────────┐ │ │ ┌─────────────────┐ │ │ ┌─────────────────┐ │
│ │ _instance: Self │ │ │ │ <<abstract>>    │ │ │ │ TypeProcessor   │ │
│ │ _registry: Dict │ │ │ │                 │ │ │ │        ↓        │ │
│ └─────────────────┘ │ │ │ + create_string │ │ │ │ ConstraintProc. │ │
│                     │ │ │ + create_number │ │ │ │        ↓        │ │
│ + get_factory()     │ │ │ + create_date   │ │ │ │ SemanticProc.   │ │
│ + register_factory()│ │ │ + create_spec.  │ │ │ │        ↓        │ │
└─────────────────────┘ │ └─────────────────┘ │ │ │ BusinessRuleProc│ │
                        │                     │ │ └─────────────────┘ │
                        │ ┌─────────────────┐ │ └─────────────────────┘
                        │ │ Standard        │ │
                        │ │ GeneratorFactory│ │
                        │ └─────────────────┘ │
                        │ ┌─────────────────┐ │
                        │ │ DWP             │ │
                        │ │ GeneratorFactory│ │
                        │ └─────────────────┘ │
                        └─────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              STRATEGY PATTERN                                  │
│                             DataGenerator                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│ <<abstract>>                                                                    │
│ + generate(constraints: Dict) → Any                                             │
│ + validate_constraints(constraints: Dict) → bool                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────┐  │
│ │StringGen.   │ │NumberGen.   │ │DateGen.     │ │ContactGen.  │ │UUIDGen.  │  │
│ │             │ │             │ │             │ │             │ │          │  │
│ │+generate()  │ │+generate()  │ │+generate()  │ │+generate()  │ │+generate │  │
│ │             │ │             │ │             │ │             │ │          │  │
│ │Strategies:  │ │Strategies:  │ │Strategies:  │ │Strategies:  │ │Strategy: │  │
│ │• Enum       │ │• Integer    │ │• Date       │ │• Email      │ │• UUID4   │  │
│ │• Pattern    │ │• Float      │ │• DateTime   │ │• Phone      │ │          │  │
│ │• Random     │ │• Range      │ │• Range      │ │• URL        │ │          │  │
│ │• Name Lists │ │             │ │             │ │             │ │          │  │
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ └──────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Facade Pattern Detail

```
CLIENT                          FACADE                         SUBSYSTEMS
┌────────┐     Simple API      ┌─────────────────┐            ┌─────────────┐
│        │────────────────────▶│                 │───────────▶│ Generator   │
│ User   │                     │ SchemaData      │            │ Registry    │
│ Code   │ data = generator    │ Generator       │            │ (Singleton) │
│        │   .generate_from    │                 │            └─────────────┘
│        │   _schema(schema,   │ Hides complex   │            ┌─────────────┐
│        │   1000)             │ interactions    │───────────▶│ Factory     │
│        │                     │ between         │            │ Selection   │
│        │                     │ subsystems      │            └─────────────┘
│        │                     │                 │            ┌─────────────┐
│        │                     │                 │───────────▶│ Schema      │
│        │                     │                 │            │ Processing  │
│        │                     │                 │            │ Chain       │
└────────┘                     └─────────────────┘            └─────────────┘
```

## Chain of Responsibility Pattern Detail

```
ProcessingContext flows through the chain:

Input: field_name="nino", field_schema={"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"}

┌─────────────────┐
│ TypeProcessor   │
├─────────────────┤
│ Maps types to   │     context.processed_constraints["generator_type"] = "string"
│ generator types │     context.processed_constraints["original_type"] = "string"
└─────┬───────────┘
      │ next
      ▼
┌─────────────────┐
│ConstraintProc.  │
├─────────────────┤
│ Extracts and    │     context.processed_constraints["pattern"] = "^[A-Z]{2}[0-9]{6}[A-D]$"
│ validates       │     Validates pattern format
│ constraints     │
└─────┬───────────┘
      │ next
      ▼
┌─────────────────┐
│ SemanticProc.   │
├─────────────────┤
│ Recognizes      │     Detects "nino" in field name
│ field semantics │     Confirms NINO pattern requirements
└─────┬───────────┘
      │ next
      ▼
┌─────────────────┐
│BusinessRuleProc.│
├─────────────────┤
│ Applies domain  │     context.processed_constraints["test_only"] = True
│ specific rules  │     context.processed_constraints["safety_validated"] = True
│ (DWP safety)    │     Ensures NINO safety compliance
└─────────────────┘

Output: Valid processing context with all constraints and safety flags
```

## Strategy Pattern Detail

```
CONTEXT                    STRATEGY INTERFACE              CONCRETE STRATEGIES

┌─────────────────┐       ┌─────────────────┐             ┌─────────────────┐
│ Generator       │       │ DataGenerator   │             │ StringGenerator │
│ Selection       │──────▶│ <<abstract>>    │◄────────────│                 │
│                 │       │                 │             │ Strategies:     │
│ def _get_gen... │       │ + generate()    │             │ • enum_strategy │
│   if type=="str"│       │ + validate()    │             │ • pattern_strat │
│   return String │       │                 │             │ • random_strat  │
│   Generator()   │       └─────────────────┘             │ • name_strat    │
│                 │                 ▲                     └─────────────────┘
│ generator.      │                 │                     ┌─────────────────┐
│ generate(       │                 │                     │ NumberGenerator │
│   constraints)  │                 └─────────────────────│                 │
└─────────────────┘                                       │ Strategies:     │
                                                          │ • integer_strat │
Algorithm Selection:                                      │ • float_strat   │
- Runtime strategy choice                                 │ • range_strat   │
- Based on field type                                     └─────────────────┘
- Interchangeable algorithms                              ┌─────────────────┐
                                                          │ DateGenerator   │
                                                          │                 │
                                                          │ Strategies:     │
                                                          │ • date_strat    │
                                                          │ • datetime_strat│
                                                          │ • range_strat   │
                                                          └─────────────────┘
```

## Template Method Pattern Detail

```
ABSTRACT CLASS                    CONCRETE IMPLEMENTATION

┌─────────────────────────────┐   ┌──────────────────────────────┐
│ SchemaDataGenerator         │   │ Implementation Steps         │
├─────────────────────────────┤   ├──────────────────────────────┤
│ generate_from_schema():     │   │ 1. Validate schema structure │
│                             │   │    ✓ Fixed algorithm step    │
│ 1. validate_schema()        │   │                              │
│    [FIXED STEP]             │   │ 2. Extract properties        │
│                             │   │    ✓ Fixed algorithm step    │
│ 2. extract_properties()     │   │                              │
│    [FIXED STEP]             │   │ 3. Generate records (HOOK)   │
│                             │   │    ● Customizable step       │
│ 3. generate_records()       │   │    ● Calls _generate_single  │
│    [HOOK - CUSTOMIZABLE]    │   │                              │
│                             │   │ 4. Return results            │
│ 4. return_results()         │   │    ✓ Fixed algorithm step    │
│    [FIXED STEP]             │   │                              │
└─────────────────────────────┘   └──────────────────────────────┘

CUSTOMIZABLE HOOK METHOD:

_generate_single_record():
┌──────────────────────────────────────────────────────────────────┐
│ FOR each field in properties:                                    │
│                                                                  │
│   IF field not required AND _should_skip_optional_field():      │
│     [HOOK - can customize skip logic]                           │
│     continue                                                     │
│                                                                  │
│   value = generate_field_value(field_name, field_schema)         │
│     [HOOK - can customize generation strategy]                  │
│                                                                  │
│   record[field_name] = value                                     │
│                                                                  │
│ RETURN record                                                    │
└──────────────────────────────────────────────────────────────────┘
```

## Factory Method Pattern Detail

```
CREATOR                    FACTORY METHOD              PRODUCTS

┌─────────────────┐       ┌─────────────────┐        ┌─────────────────┐
│GeneratorFactory │       │create_specialized│       │ ContactGenerator│
│                 │       │_generator(type)  │       │                 │
│ + create_string │       │                 │       │ Used for:       │
│ + create_number │       │ type_map = {    │───────┤ • email         │
│ + create_date   │       │   "contact": -> │       │ • phone         │
│ + create_spec.. │       │   "boolean": -> │       │ • url           │
└─────────────────┘       │   "uuid": ->    │       └─────────────────┘
        ▲                 │   "email": ->   │       ┌─────────────────┐
        │                 │   "phone": ->   │       │ BooleanGenerator│
        │                 │   "url": ->     │       │                 │
┌─────────────────┐       │ }               │───────┤ Used for:       │
│StandardFactory  │       │                 │       │ • boolean fields│
│                 │       │ return type_map │       │ • flags         │
│ Implements all  │       │   [type]()      │       └─────────────────┘
│ factory methods │       └─────────────────┘       ┌─────────────────┐
└─────────────────┘                                 │ UUIDGenerator   │
┌─────────────────┐        Benefits:                │                 │
│ DWPFactory      │        • Encapsulates object    │ Used for:       │
│                 │          creation               │ • id fields     │
│ Extends standard│        • Easy to add new types  │ • unique refs   │
│ with DWP rules  │        • Centralized mapping    └─────────────────┘
└─────────────────┘
```

## Singleton Pattern Detail

```
SINGLETON IMPLEMENTATION

┌─────────────────────────────────────────────────────────────────────┐
│ class GeneratorRegistry:                                            │
│                                                                     │
│     _instance = None                                                │
│     _registry = {}                                                  │
│                                                                     │
│     def __new__(cls):                                               │
│         if cls._instance is None:                                   │
│             cls._instance = super().__new__(cls)                    │
│             cls._instance._initialize_registry()                    │
│         return cls._instance                                        │
│                                                                     │
│     def _initialize_registry(self):                                 │
│         self._registry = {                                          │
│             "standard": StandardGeneratorFactory(),                 │
│             "dwp": DWPGeneratorFactory()                            │
│         }                                                           │
└─────────────────────────────────────────────────────────────────────┘

USAGE ACROSS APPLICATION:

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Component A     │    │ Component B     │    │ Component C     │
│                 │    │                 │    │                 │
│ registry =      │    │ registry =      │    │ registry =      │
│ Generator       │    │ Generator       │    │ Generator       │
│ Registry()      │    │ Registry()      │    │ Registry()      │
│                 │    │                 │    │                 │
└─────┬───────────┘    └─────┬───────────┘    └─────┬───────────┘
      │                      │                      │
      └──────────────────────┼──────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ SAME INSTANCE   │
                    │                 │
                    │ • Shared state  │
                    │ • Global access │
                    │ • Single source │
                    │   of truth      │
                    └─────────────────┘
```

## Builder Pattern Detail

```
DIRECTOR              BUILDER                 PRODUCT

┌─────────────────┐   ┌─────────────────┐    ┌─────────────────┐
│SchemaProcessor │   │ Chain Builder   │    │ Processing      │
│Chain            │   │                 │    │ Chain           │
│                 │   │ def _build_     │    │                 │
│ def __init__(): │   │ chain():        │    │ TypeProcessor   │
│   self._chain = │──▶│                 │───▶│       ↓         │
│   self._build_  │   │ 1. Create       │    │ ConstraintProc  │
│   chain()       │   │    processors   │    │       ↓         │
│                 │   │                 │    │ SemanticProc    │
│                 │   │ 2. Link them    │    │       ↓         │
│                 │   │    together     │    │ BusinessRule    │
│                 │   │                 │    │ Processor       │
│                 │   │ 3. Return head  │    │                 │
└─────────────────┘   └─────────────────┘    └─────────────────┘

STEP-BY-STEP CONSTRUCTION:

Step 1: Create Components
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ TypeProcessor() │ │ ConstraintProc()│ │ SemanticProc()  │ │ BusinessRule()  │
└─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘

Step 2: Chain Linking (Fluent Interface)
type_processor.set_next(constraint_processor)
             .set_next(semantic_processor)
             .set_next(business_rule_processor)

Step 3: Return Complete Chain
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ TypeProcessor   │───▶│ ConstraintProc  │───▶│ SemanticProc    │───▶│ BusinessRule    │
│ (head)          │    │                 │    │                 │    │ (tail)          │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

This comprehensive diagram system shows how all the design patterns work together to create a flexible, maintainable, and extensible data generation system. Each pattern solves specific problems while working harmoniously with the others.