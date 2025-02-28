from elasticquerydsl.base import DSLQuery
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.builder.base import BaseQueryEngine
from elastictoolkit.queryutils.builder.matchdirectivebuilder import (
    MatchDirectiveBuilder,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective


class DirectiveEngine(BaseQueryEngine):
    def to_dsl(self) -> DSLQuery:
        engine_attributes = self.get_engine_attr(MatchDirective)
        match_directive_config = (
            getattr(self.Config, "match_directive_config", None)
            or self._DefaultConfig.match_directive_config
        )

        bool_builder = BooleanDSLBuilder()
        for attr, match_directive in engine_attributes.items():
            match_directive = (
                MatchDirectiveBuilder(match_directive)
                .set_value_mapper(attr, self.Config.value_mapper)
                .set_match_params(self.match_params)
                .set_parent_engine_name(self.__class__.__name__)
                .set_match_directive_config(**match_directive_config)
                .build()
            )
            # Generate Query
            match_directive.execute(bool_builder)
        return bool_builder.build()
