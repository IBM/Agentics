from .dynamic_information_extraction.application import DynamicInformationExtractionApp
from .macro_economic_impact.application import MacroEconomicImpactApp
from .text2sql.application import Text2SQLApp
from .qa.application import QAApp

from agentics.api.core.registry import registry

registry.register(DynamicInformationExtractionApp())
registry.register(MacroEconomicImpactApp())
registry.register(Text2SQLApp())
registry.register(QAApp())
