"""
Schema Processor - Chain of Responsibility Pattern
Processes schema definitions and applies business rules
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class ProcessingContext:
    """
    Context object that flows through the chain
    Contains all information needed for processing
    """
    field_name: str
    field_schema: Dict
    schema_context: Dict
    constraints: Dict
    processed_constraints: Dict
    errors: List[str]


class SchemaProcessor(ABC):
    """
    Abstract handler in Chain of Responsibility pattern
    Each processor handles specific aspects of schema processing
    """

    def __init__(self):
        self._next_processor: Optional[SchemaProcessor] = None

    def set_next(self, processor: 'SchemaProcessor') -> 'SchemaProcessor':
        """Set the next processor in the chain"""
        self._next_processor = processor
        return processor

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Process the context and pass to next processor
        Template Method Pattern - defines the processing skeleton
        """
        # Process current step
        self._handle_processing(context)

        # Pass to next processor if exists
        if self._next_processor:
            return self._next_processor.process(context)

        return context

    @abstractmethod
    def _handle_processing(self, context: ProcessingContext):
        """Handle the specific processing step"""
        pass


class TypeProcessor(SchemaProcessor):
    """
    Concrete processor - handles type mapping and validation
    """

    TYPE_MAPPING = {
        "string": "string",
        "integer": "integer",
        "number": "number",
        "float": "number",
        "boolean": "boolean",
        "date": "date",
        "datetime": "date",
        "email": "contact",
        "phone": "contact",
        "url": "contact",
        "uuid": "uuid"
    }

    def _handle_processing(self, context: ProcessingContext):
        """Process type information"""
        field_type = context.field_schema.get("type", "string")

        # Map type to generator type
        generator_type = self.TYPE_MAPPING.get(field_type, "string")
        context.processed_constraints["generator_type"] = generator_type
        context.processed_constraints["original_type"] = field_type

        # Validate type
        if field_type not in self.TYPE_MAPPING:
            context.errors.append(f"Unknown type '{field_type}' for field '{context.field_name}'")


class ConstraintProcessor(SchemaProcessor):
    """
    Concrete processor - handles constraint validation and processing
    """

    def _handle_processing(self, context: ProcessingContext):
        """Process constraints"""
        constraints = context.constraints.copy()

        # Process numeric constraints
        if "minimum" in context.field_schema:
            constraints["minimum"] = context.field_schema["minimum"]

        if "maximum" in context.field_schema:
            constraints["maximum"] = context.field_schema["maximum"]

        # Process string constraints
        if "minLength" in context.field_schema:
            constraints["minLength"] = context.field_schema["minLength"]

        if "maxLength" in context.field_schema:
            constraints["maxLength"] = context.field_schema["maxLength"]

        # Process enum constraints
        if "enum" in context.field_schema:
            constraints["enum"] = context.field_schema["enum"]

        # Process pattern constraints
        if "pattern" in context.field_schema:
            constraints["pattern"] = context.field_schema["pattern"]

        # Process date constraints
        if "start" in context.field_schema:
            constraints["start"] = context.field_schema["start"]

        if "end" in context.field_schema:
            constraints["end"] = context.field_schema["end"]

        # Validate constraints
        self._validate_constraints(context, constraints)

        context.processed_constraints.update(constraints)

    def _validate_constraints(self, context: ProcessingContext, constraints: Dict):
        """Validate constraint values"""
        # Validate numeric ranges
        if "minimum" in constraints and "maximum" in constraints:
            if constraints["minimum"] > constraints["maximum"]:
                context.errors.append(
                    f"Invalid range for field '{context.field_name}': "
                    f"minimum ({constraints['minimum']}) > maximum ({constraints['maximum']})"
                )

        # Validate string length ranges
        if "minLength" in constraints and "maxLength" in constraints:
            if constraints["minLength"] > constraints["maxLength"]:
                context.errors.append(
                    f"Invalid length range for field '{context.field_name}': "
                    f"minLength ({constraints['minLength']}) > maxLength ({constraints['maxLength']})"
                )


class SemanticProcessor(SchemaProcessor):
    """
    Concrete processor - handles semantic field recognition
    Uses field names to infer additional constraints
    """

    SEMANTIC_MAPPINGS = {
        "email": {"contact_type": "email"},
        "phone": {"contact_type": "phone"},
        "url": {"contact_type": "url"},
        "website": {"contact_type": "url"},
        "first_name": {"data_type": "first_names"},
        "firstname": {"data_type": "first_names"},
        "last_name": {"data_type": "last_names"},
        "lastname": {"data_type": "last_names"},
        "surname": {"data_type": "last_names"},
        "company": {"data_type": "companies"},
        "city": {"data_type": "cities"},
        "country": {"data_type": "countries"},
        "nino": {"pattern": "^[A-Z]{2}[0-9]{6}[A-D]$", "generator_type": "string"},
        "ni_number": {"pattern": "^[A-Z]{2}[0-9]{6}[A-D]$", "generator_type": "string"},
    }

    def _handle_processing(self, context: ProcessingContext):
        """Process semantic field information"""
        field_name_lower = context.field_name.lower()

        # Check for semantic matches
        for pattern, mappings in self.SEMANTIC_MAPPINGS.items():
            if pattern in field_name_lower:
                context.processed_constraints.update(mappings)
                break

        # Special handling for ID fields
        if field_name_lower == "id" or field_name_lower.endswith("_id"):
            context.processed_constraints["generator_type"] = "uuid"

        # Special handling for date fields
        if "date" in field_name_lower and context.processed_constraints.get("original_type") == "string":
            context.processed_constraints["generator_type"] = "date"


class BusinessRuleProcessor(SchemaProcessor):
    """
    Concrete processor - applies business rules specific to domain
    """

    def _handle_processing(self, context: ProcessingContext):
        """Apply business rules"""
        # DWP-specific business rules
        if context.schema_context.get("domain") == "dwp":
            self._apply_dwp_rules(context)

    def _apply_dwp_rules(self, context: ProcessingContext):
        """Apply DWP-specific business rules"""
        field_name = context.field_name.lower()

        # NINO safety rules
        if "nino" in field_name:
            context.processed_constraints["test_only"] = True
            context.processed_constraints["safety_validated"] = True

        # Age validation for benefits
        if "age" in field_name:
            # Ensure reasonable age ranges for benefit claimants
            if "minimum" not in context.processed_constraints:
                context.processed_constraints["minimum"] = 16
            if "maximum" not in context.processed_constraints:
                context.processed_constraints["maximum"] = 100

        # Payment amount validation
        if "payment" in field_name or "amount" in field_name:
            if "minimum" not in context.processed_constraints:
                context.processed_constraints["minimum"] = 0.01
            context.processed_constraints["decimalPlaces"] = 2


class SchemaProcessorChain:
    """
    Facade Pattern - Provides simple interface to complex chain processing
    Builder Pattern - Builds the processing chain
    """

    def __init__(self, domain: str = "standard"):
        self.domain = domain
        self._chain = self._build_chain()

    def _build_chain(self) -> SchemaProcessor:
        """
        Build the processing chain
        Builder Pattern - constructs complex object step by step
        """
        # Create processors
        type_processor = TypeProcessor()
        constraint_processor = ConstraintProcessor()
        semantic_processor = SemanticProcessor()
        business_rule_processor = BusinessRuleProcessor()

        # Chain them together
        type_processor.set_next(constraint_processor) \
                     .set_next(semantic_processor) \
                     .set_next(business_rule_processor)

        return type_processor

    def process_field(self, field_name: str, field_schema: Dict, schema_context: Dict = None) -> Dict:
        """
        Process a single field through the chain
        Facade Pattern - provides simple interface
        """
        schema_context = schema_context or {"domain": self.domain}

        # Create processing context
        context = ProcessingContext(
            field_name=field_name,
            field_schema=field_schema,
            schema_context=schema_context,
            constraints={},
            processed_constraints={},
            errors=[]
        )

        # Process through chain
        result_context = self._chain.process(context)

        # Return processed result
        return {
            "constraints": result_context.processed_constraints,
            "errors": result_context.errors,
            "valid": len(result_context.errors) == 0
        }