# API Applications
from .dynamic_information_extraction.application import DynamicInformationExtractionApp
from .macro_economic_impact.application import MacroEconomicImpactApp

from agentics.api.core.registry import registry

registry.register(DynamicInformationExtractionApp())
registry.register(MacroEconomicImpactApp())
