Feature: WQX XML to tabular representation and pandas DataFrames
    In order to convert raw WQX XML into pandas DataFrames
    As a data consumer with some knowledge of Python
    I want to map WQX XML to tabular form and create pandas DataFrames from that

    # this is effectively a statement of wqx_mappings module invariants at design time
    Scenario: Tabular definitions and mappings must be valid and consistent
        Given "wqx_mappings.column_mappings" as the authoritative XML-to-column definition
        And "wqx_mappings.context_descriptors" as the authoritative context node descriptor
        And "wqx_mappings.tabular_defs" as the authoritative tabular format descriptor
        And "wqx_mappings.context_xpaths" as the definitive context node XPath expression set
        And "wqx_mappings.val_xpaths" as the definitive column value XPath expression set
        Then the tabular format descriptor must be contained in the XML-to-column definition
        And the column value XPath expression set must be contained in the XML-to-column definition

		



