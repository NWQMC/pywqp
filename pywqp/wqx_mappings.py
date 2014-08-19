import pandas
from lxml import etree as et

'''
MAPPING WQX TO ITS CANONICAL TABULAR FORM

The canonical tabular form of WQX data is defined as a sequence of columns.
This representation is used to structure the CSV and TSV downloads, and is
the basis for importing WQX into a pandas dataframe. 

For the purposes of canonical tabular representation, a column 
is considered to map uniquely to a particular semantic data definition.
Since the definitive source of the data definitions is the WQX schema, the
mapping to the canonical tabular form preserves the definitions completely.

The mappings between WQX XML and the canonical tabular form are logically
bidirectional: they define a lossless two-way logical transformation for
all *non-empty* data elements. (It's possible to include arbitrary empty
XML tags that are schema-compliant but that do not affect the derived
tabular form. The tabular form is sparse by design: any row will contain
empty columns if the corresponding data item is not present.)

The core structure of the mappings is four kinds of Logical XML Nodes:

    Organization
    MonitoringLocation
    Activity
    Result

Each Logical Node definition corresponds to a set of descendant nodes that can be 
is selected by a relative XPath expression. Each descendant node so selected
corresponds to a data semantic and to a column.

The Logical Nodes also may contain other logical nodes as descendants. The 
nesting relationship of Logical Nodes is as follows:

    Organization nodes contain MonitoringLocation and Activity descendant
    nodesets.

    Activity nodes contain Result nodesets.

    Neither MonitoringLocation nor Result nodes contain logical descendants.

The presence of a MonitoringLocation node (for station information) or a 
Result node (for Result information) corresponds to a data row in the 
tabular form.

TWO KINDS OF XPATH EXPRESSIONS IN MAPPINGS

A NODESET XPath expression selects Logical Nodes:

    The Organization Nodeset expression is absolute (or relative to the 
    document root, which is effectively the same thing.)

    The MonitoringLocation and Activity expressions are relative to
    a single Organization node.

    The Result expression is relative to a single Activity node.

A COLUMN VALUE expression selects a value for an individual column within
the context of the current Logical Node.


 Putting it all together, here's how the queries define column-specific info from the XML:

 Organizations: obtained by Nodeset XPath scoped to root
   |
   |
   |__Organization cols: Column Value XPaths scoped to an Organization node
   |
   |
   |__MonitoringLocations: obtained by Nodeset XPath scoped to an Organization node
   |   |
   |   |
   |   |__MonitoringLocation cols: Column Value XPaths scoped to a MonitoringLocation node
   |
   |
   |__Activities: obtained by Nodeset XPath scoped to an Organization
       |
       |
       |__Activity cols: Column Value XPaths scoped to an Activity node
       |
       |
       |__Results: obtained by Nodeset XPath scoped to an Activity node           |
           |
           |
           |__Result cols: Column Value XPaths scoped to a Result node

'''

class WQXMapper:

    # the ordered column names of the tabular form of a /Station/search dataset
    station_cols = (
        'OrganizationIdentifier',
        'OrganizationFormalName',
        'MonitoringLocationIdentifier',
        'MonitoringLocationName',
        'MonitoringLocationTypeName',
        'MonitoringLocationDescriptionText',
        'HUCEightDigitCode',
        'DrainageAreaMeasure/MeasureValue',
        'DrainageAreaMeasure/MeasureUnitCode',
        'ContributingDrainageAreaMeasure/MeasureValue',
        'ContributingDrainageAreaMeasure/MeasureUnitCode',
        'LatitudeMeasure',
        'LongitudeMeasure',
        'SourceMapScaleNumeric',
        'HorizontalAccuracyMeasure/MeasureValue',
        'HorizontalAccuracyMeasure/MeasureUnitCode',
        'HorizontalCollectionMethodName',
        'HorizontalCoordinateReferenceSystemDatumName',
        'VerticalMeasure/MeasureValue',
        'VerticalMeasure/MeasureUnitCode',
        'VerticalAccuracyMeasure/MeasureValue',
        'VerticalAccuracyMeasure/MeasureUnitCode',
        'VerticalCollectionMethodName',
        'VerticalCoordinateReferenceSystemDatumName',
        'CountryCode',
        'StateCode',
        'CountyCode',
        'AquiferName',
        'FormationTypeText',
        'AquiferTypeName',
        'ConstructionDateText',
        'WellDepthMeasure/MeasureValue',
        'WellDepthMeasure/MeasureUnitCode',
        'WellHoleDepthMeasure/MeasureValue',
        'WellHoleDepthMeasure/MeasureUnitCode')

    # the ordered column names of the tabular form of a /Result/search dataset
    result_cols = (
        'OrganizationIdentifier',
        'OrganizationFormalName',
        'ActivityIdentifier',
        'ActivityTypeCode',
        'ActivityMediaName',
        'ActivityMediaSubdivisionName',
        'ActivityStartDate',
        'ActivityStartTime/Time',
        'ActivityStartTime/TimeZoneCode',
        'ActivityEndDate',
        'ActivityEndTime/Time',
        'ActivityEndTime/TimeZoneCode',
        'ActivityDepthHeightMeasure/MeasureValue',
        'ActivityDepthHeightMeasure/MeasureUnitCode',
        'ActivityDepthAltitudeReferencePointText',
        'ActivityTopDepthHeightMeasure/MeasureValue',
        'ActivityTopDepthHeightMeasure/MeasureUnitCode',
        'ActivityBottomDepthHeightMeasure/MeasureValue',
        'ActivityBottomDepthHeightMeasure/MeasureUnitCode',
        'ProjectIdentifier',
        'ActivityConductingOrganizationText',
        'MonitoringLocationIdentifier',
        'ActivityCommentText',
        'SampleAquifer',
        'HydrologicCondition',
        'HydrologicEvent',
        'SampleCollectionMethod/MethodIdentifier',
        'SampleCollectionMethod/MethodIdentifierContext',
        'SampleCollectionMethod/MethodName',
        'SampleCollectionEquipmentName',
        'ResultDetectionConditionText',
        'CharacteristicName',
        'ResultSampleFractionText',
        'ResultMeasureValue',
        'ResultMeasure/MeasureUnitCode',
        'MeasureQualifierCode',
        'ResultStatusIdentifier',
        'StatisticalBaseCode',
        'ResultValueTypeName',
        'ResultWeightBasisText',
        'ResultTimeBasisText',
        'ResultTemperatureBasisText',
        'ResultParticleSizeBasisText',
        'PrecisionValue',
        'ResultCommentText',
        'USGSPCode',
        'ResultDepthHeightMeasure/MeasureValue',
        'ResultDepthHeightMeasure/MeasureUnitCode',
        'ResultDepthAltitudeReferencePointText',
        'SubjectTaxonomicName',
        'SampleTissueAnatomyName',
        'ResultAnalyticalMethod/MethodIdentifier',
        'ResultAnalyticalMethod/MethodIdentifierContext',
        'ResultAnalyticalMethod/MethodName',
        'MethodDescriptionText',
        'LaboratoryName',
        'AnalysisStartDate',
        'ResultLaboratoryCommentText',
        'DetectionQuantitationLimitTypeName',
        'DetectionQuantitationLimitMeasure/MeasureValue',
        'DetectionQuantitationLimitMeasure/MeasureUnitCode',
        'PreparationStartDate')


    # A formal statement of the mapping between column name and the tagnames
    # of XML steps. Not used in code, but all paths relevant to /Station/search
    # data were derived from this and must be consistent with this.
    # Assumption: all tagnames are in the WQX namespace
    station_step_mappings = {"OrganizationIdentifier": ("WQX", "Organization", "OrganizationDescription", "OrganizationIdentifier"),
        "OrganizationFormalName": ("WQX", "Organization", "OrganizationDescription", "OrganizationFormalName"),
        "MonitoringLocationIdentifier": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "MonitoringLocationIdentifier"),
        "MonitoringLocationName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "MonitoringLocationName"),
        "MonitoringLocationTypeName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "MonitoringLocationTypeName"),
        "MonitoringLocationDescriptionText": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "MonitoringLocationDescriptionText"),
        "HUCEightDigitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "HUCEightDigitCode"),
        "DrainageAreaMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "DrainageAreaMeasure", "MeasureValue"),
        "DrainageAreaMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "DrainageAreaMeasure", "MeasureUnitCode"),
        "ContributingDrainageAreaMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "ContributingDrainageAreaMeasure", "MeasureValue"),
        "ContributingDrainageAreaMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationIdentity", "ContributingDrainageAreaMeasure", "MeasureUnitCode"),
        "LatitudeMeasure": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "LatitudeMeasure"),
        "LongitudeMeasure": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "LongitudeMeasure"),
        "SourceMapScaleNumeric": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "SourceMapScaleNumeric"),
        "HorizontalAccuracyMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "HorizontalAccuracyMeasure", "MeasureValue"),
        "HorizontalAccuracyMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "HorizontalAccuracyMeasure", "MeasureUnitCode"),
        "HorizontalCollectionMethodName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "HorizontalCollectionMethodName"),
        "HorizontalCoordinateReferenceSystemDatumName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "HorizontalCoordinateReferenceSystemDatumName"),
        "VerticalMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalMeasure", "MeasureValue"),
        "VerticalMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalMeasure", "MeasureUnitCode"),
        "VerticalAccuracyMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalAccuracyMeasure", "MeasureValue"),
        "VerticalAccuracyMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalAccuracyMeasure", "MeasureUnitCode"),
        "VerticalCollectionMethodName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalCollectionMethodName"),
        "VerticalCoordinateReferenceSystemDatumName": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "VerticalCoordinateReferenceSystemDatumName"),
        "CountryCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "CountryCode"),
        "StateCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "StateCode"),
        "CountyCode": ("WQX", "Organization", "MonitoringLocation", "MonitoringLocationGeospatial", "CountyCode"),
        "AquiferName": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "AquiferName"),
        "FormationTypeText": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "FormationTypeText"),
        "AquiferTypeName": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "AquiferTypeName"),
        "ConstructionDateText": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "ConstructionDateText"),
        "WellDepthMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "WellDepthMeasure", "MeasureValue"),
        "WellDepthMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "WellDepthMeasure", "MeasureUnitCode"),
        "WellHoleDepthMeasure/MeasureValue": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "WellHoleDepthMeasure", "MeasureValue"),
        "WellHoleDepthMeasure/MeasureUnitCode": ("WQX", "Organization", "MonitoringLocation", "WellInformation", "WellHoleDepthMeasure", "MeasureUnitCode")}


    # A formal statement of the mapping between column name and the tagnames
    # of XML steps. Not used in code, but all paths relevant to /Result/search
    # data were derived from this and must be consistent with this.
    # Assumption: all tagnames are in the WQX namespace
    result_step_mappings = {"OrganizationIdentifier": ("WQX", "Organization", "OrganizationDescription", "OrganizationIdentifier"),
        "OrganizationFormalName": ("WQX", "Organization", "OrganizationDescription", "OrganizationFormalName"),
        "ActivityIdentifier": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityIdentifier"),
        "ActivityTypeCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityTypeCode"),
        "ActivityMediaName": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityMediaName"),
        "ActivityMediaSubdivisionName": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityMediaSubdivisionName"),
        "ActivityStartDate": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityStartDate"),
        "ActivityStartTime/Time": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityStartTime", "Time"),
        "ActivityStartTime/TimeZoneCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityStartTime", "TimeZoneCode"),
        "ActivityEndDate": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityEndDate"),
        "ActivityEndTime/Time": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityEndTime", "Time"),
        "ActivityEndTime/TimeZoneCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityEndTime", "TimeZoneCode"),
        "ActivityDepthHeightMeasure/MeasureValue": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityDepthHeightMeasure", "MeasureValue"),
        "ActivityDepthHeightMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityDepthHeightMeasure", "MeasureUnitCode"),
        "ActivityDepthAltitudeReferencePointText": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityDepthAltitudeReferencePointText"),
        "ActivityTopDepthHeightMeasure/MeasureValue": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityTopDepthHeightMeasure", "MeasureValue"),
        "ActivityTopDepthHeightMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityTopDepthHeightMeasure", "MeasureUnitCode"),
        "ActivityBottomDepthHeightMeasure/MeasureValue": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityBottomDepthHeightMeasure", "MeasureValue"),
        "ActivityBottomDepthHeightMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityBottomDepthHeightMeasure", "MeasureUnitCode"),
        "ProjectIdentifier": ("WQX", "Organization", "Activity", "ActivityDescription", "ProjectIdentifier"),
        "ActivityConductingOrganizationText": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityConductingOrganizationText"),
        "MonitoringLocationIdentifier": ("WQX", "Organization", "Activity", "ActivityDescription", "MonitoringLocationIdentifier"),
        "ActivityCommentText": ("WQX", "Organization", "Activity", "ActivityDescription", "ActivityCommentText"),
        "SampleAquifer": ("WQX", "Organization", "Activity", "ActivityDescription", "SampleAquifer"),
        "HydrologicCondition": ("WQX", "Organization", "Activity", "ActivityDescription", "HydrologicCondition"),
        "HydrologicEvent": ("WQX", "Organization", "Activity", "ActivityDescription", "HydrologicEvent"),
        "SampleCollectionMethod/MethodIdentifier": ("WQX", "Organization", "Activity", "SampleDescription", "SampleCollectionMethod", "MethodIdentifier"),
        "SampleCollectionMethod/MethodIdentifierContext": ("WQX", "Organization", "Activity", "SampleDescription", "SampleCollectionMethod", "MethodIdentifierContext"),
        "SampleCollectionMethod/MethodName": ("WQX", "Organization", "Activity", "SampleDescription", "SampleCollectionMethod", "MethodName"),
        "SampleCollectionEquipmentName": ("WQX", "Organization", "Activity", "SampleDescription", "SampleCollectionEquipmentName"),
        "ResultDetectionConditionText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultDetectionConditionText"),
        "CharacteristicName": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "CharacteristicName"),
        "ResultSampleFractionText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultSampleFractionText"),
        "ResultMeasureValue": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultMeasure", "ResultMeasureValue"),
        "ResultMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultMeasure", "MeasureUnitCode"),
        "MeasureQualifierCode": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultMeasure", "MeasureQualifierCode"),
        "ResultStatusIdentifier": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultStatusIdentifier"),
        "StatisticalBaseCode": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "StatisticalBaseCode"),
        "ResultValueTypeName": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultValueTypeName"),
        "ResultWeightBasisText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultWeightBasisText"),
        "ResultTimeBasisText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultTimeBasisText"),
        "ResultTemperatureBasisText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultTemperatureBasisText"),
        "ResultParticleSizeBasisText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultParticleSizeBasisText"),
        "PrecisionValue": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "DataQuality", "PrecisionValue"),
        "ResultCommentText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultCommentText"),
        "USGSPCode": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "USGSPCode"),
        "ResultDepthHeightMeasure/MeasureValue": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultDepthHeightMeasure", "MeasureValue"),
        "ResultDepthHeightMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultDepthHeightMeasure", "MeasureUnitCode"),
        "ResultDepthAltitudeReferencePointText": ("WQX", "Organization", "Activity", "Result", "ResultDescription", "ResultDepthAltitudeReferencePointText"),
        "SubjectTaxonomicName": ("WQX", "Organization", "Activity", "Result", "BiologicalResultDescription", "SubjectTaxonomicName"),
        "SampleTissueAnatomyName": ("WQX", "Organization", "Activity", "Result", "BiologicalResultDescription", "SampleTissueAnatomyName"),
        "ResultAnalyticalMethod/MethodIdentifier": ("WQX", "Organization", "Activity", "Result", "ResultAnalyticalMethod", "MethodIdentifier"),
        "ResultAnalyticalMethod/MethodIdentifierContext": ("WQX", "Organization", "Activity", "Result", "ResultAnalyticalMethod", "MethodIdentifierContext"),
        "ResultAnalyticalMethod/MethodName": ("WQX", "Organization", "Activity", "Result", "ResultAnalyticalMethod", "MethodName"),
        "MethodDescriptionText": ("WQX", "Organization", "Activity", "Result", "ResultAnalyticalMethod", "MethodDescriptionText"),
        "LaboratoryName": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "LaboratoryName"),
        "AnalysisStartDate": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "AnalysisStartDate"),
        "ResultLaboratoryCommentText": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "ResultLaboratoryCommentText"),
        "DetectionQuantitationLimitTypeName": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "ResultDetectionQuantitationLimit", "DetectionQuantitationLimitTypeName"),
        "DetectionQuantitationLimitMeasure/MeasureValue": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "ResultDetectionQuantitationLimit", "DetectionQuantitationLimitMeasure", "MeasureValue"),
        "DetectionQuantitationLimitMeasure/MeasureUnitCode": ("WQX", "Organization", "Activity", "Result", "ResultLabInformation", "ResultDetectionQuantitationLimit", "DetectionQuantitationLimitMeasure", "MeasureUnitCode"),
        "PreparationStartDate": ("WQX", "Organization", "Activity", "Result", "LabSamplePreparation", "PreparationStartDate")}


    # common (i.e. shared across multiple rows) column defs descended from /WQX/Organization
    # Fortunately, these column defs are identical in station and result mappings.
    # The column name is the key. The value is the RELATIVE XPath from
    # /WQX/Organization, obeying the convention that "wqx" is the 
    #  XPath expression's expected alias for the WQX namespace.
    org_col_xpaths = {
        'OrganizationIdentifier': 'wqx:OrganizationDescription/wqx:OrganizationIdentifier',
        'OrganizationFormalName': 'wqx:OrganizationDescription/wqx:OrganizationFormalName'}

    # common (i.e. shared across multiple rows) column defs descended from /WQX/Organization/Activity.
    # These column defs apply to result mappings and not to stations.
    # The column name is the key. The value is the RELATIVE path from
    # /WQX/Organization/Activity, obeying the convention that "wqx" is the 
    #  XPath expression's expected alias for the WQX namespace.
    activity_col_xpaths = {
        'ActivityIdentifier': 'wqx:ActivityDescription/wqx:ActivityIdentifier',
        'ActivityTopDepthHeightMeasure/MeasureUnitCode': 'wqx:ActivityDescription/wqx:ActivityTopDepthHeightMeasure/wqx:MeasureUnitCode',
        'SampleAquifer': 'wqx:ActivityDescription/wqx:SampleAquifer',
        'ActivityStartTime/Time': 'wqx:ActivityDescription/wqx:ActivityStartTime/wqx:Time',
        'ActivityStartTime/TimeZoneCode': 'wqx:ActivityDescription/wqx:ActivityStartTime/wqx:TimeZoneCode',
        'ActivityStartDate': 'wqx:ActivityDescription/wqx:ActivityStartDate',
        'ActivityEndTime/Time': 'wqx:ActivityDescription/wqx:ActivityEndTime/wqx:Time',
        'ActivityConductingOrganizationText': 'wqx:ActivityDescription/wqx:ActivityConductingOrganizationText',
        'ActivityBottomDepthHeightMeasure/MeasureUnitCode': 'wqx:ActivityDescription/wqx:ActivityBottomDepthHeightMeasure/wqx:MeasureUnitCode',
        'ActivityTypeCode': 'wqx:ActivityDescription/wqx:ActivityTypeCode',
        'ActivityDepthHeightMeasure/MeasureValue': 'wqx:ActivityDescription/wqx:ActivityDepthHeightMeasure/wqx:MeasureValue',
        'ActivityDepthAltitudeReferencePointText': 'wqx:ActivityDescription/wqx:ActivityDepthAltitudeReferencePointText',
        'SampleCollectionMethod/MethodName': 'wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodName',
        'ActivityMediaName': 'wqx:ActivityDescription/wqx:ActivityMediaName',
        'SampleCollectionMethod/MethodIdentifier': 'wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodIdentifier',
        'ActivityTopDepthHeightMeasure/MeasureValue': 'wqx:ActivityDescription/wqx:ActivityTopDepthHeightMeasure/wqx:MeasureValue',
        'MonitoringLocationIdentifier': 'wqx:ActivityDescription/wqx:MonitoringLocationIdentifier',
        'ProjectIdentifier': 'wqx:ActivityDescription/wqx:ProjectIdentifier',
        'ActivityEndTime/TimeZoneCode': 'wqx:ActivityDescription/wqx:ActivityEndTime/wqx:TimeZoneCode',
        'HydrologicCondition': 'wqx:ActivityDescription/wqx:HydrologicCondition',
        'ActivityCommentText': 'wqx:ActivityDescription/wqx:ActivityCommentText',
        'ActivityEndDate': 'wqx:ActivityDescription/wqx:ActivityEndDate',
        'HydrologicEvent': 'wqx:ActivityDescription/wqx:HydrologicEvent',
        'ActivityBottomDepthHeightMeasure/MeasureValue': 'wqx:ActivityDescription/wqx:ActivityBottomDepthHeightMeasure/wqx:MeasureValue',
        'SampleCollectionMethod/MethodIdentifierContext': 'wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodIdentifierContext',
        'ActivityMediaSubdivisionName': 'wqx:ActivityDescription/wqx:ActivityMediaSubdivisionName',
        'SampleCollectionEquipmentName': 'wqx:SampleDescription/wqx:SampleCollectionEquipmentName',
        'ActivityDepthHeightMeasure/MeasureUnitCode': 'wqx:ActivityDescription/wqx:ActivityDepthHeightMeasure/wqx:MeasureUnitCode'}

    # item-specific (i.e. single-row-specific) column defs descended from /WQX/Organization/MonitoringLocation.
    # The column name is the key. The value is the RELATIVE XPath from
    # /WQX/Organization/MonitoringLocation, obeying the convention that "wqx" is the 
    #  XPath expression's expected alias for the WQX namespace.
    station_col_xpaths={'DrainageAreaMeasure/MeasureUnitCode': 'wqx:MonitoringLocationIdentity/wqx:DrainageAreaMeasure/wqx:MeasureUnitCode',
        'MonitoringLocationTypeName': 'wqx:MonitoringLocationIdentity/wqx:MonitoringLocationTypeName',
        'HorizontalCoordinateReferenceSystemDatumName': 'wqx:MonitoringLocationGeospatial/wqx:HorizontalCoordinateReferenceSystemDatumName',
        'DrainageAreaMeasure/MeasureValue': 'wqx:MonitoringLocationIdentity/wqx:DrainageAreaMeasure/wqx:MeasureValue',
        'StateCode': 'wqx:MonitoringLocationGeospatial/wqx:StateCode',
        'VerticalCoordinateReferenceSystemDatumName': 'wqx:MonitoringLocationGeospatial/wqx:VerticalCoordinateReferenceSystemDatumName',
        'MonitoringLocationName': 'wqx:MonitoringLocationIdentity/wqx:MonitoringLocationName',
        'CountryCode': 'wqx:MonitoringLocationGeospatial/wqx:CountryCode',
        'FormationTypeText': 'wqx:WellInformation/wqx:FormationTypeText',
        'VerticalAccuracyMeasure/MeasureUnitCode': 'wqx:MonitoringLocationGeospatial/wqx:VerticalAccuracyMeasure/wqx:MeasureUnitCode',
        'AquiferTypeName': 'wqx:WellInformation/wqx:AquiferTypeName',
        'HorizontalAccuracyMeasure/MeasureUnitCode': 'wqx:MonitoringLocationGeospatial/wqx:HorizontalAccuracyMeasure/wqx:MeasureUnitCode',
        'ContributingDrainageAreaMeasure/MeasureUnitCode': 'wqx:MonitoringLocationIdentity/wqx:ContributingDrainageAreaMeasure/wqx:MeasureUnitCode',
        'WellHoleDepthMeasure/MeasureValue': 'wqx:WellInformation/wqx:WellHoleDepthMeasure/wqx:MeasureValue',
        'WellDepthMeasure/MeasureValue': 'wqx:WellInformation/wqx:WellDepthMeasure/wqx:MeasureValue',
        'LongitudeMeasure': 'wqx:MonitoringLocationGeospatial/wqx:LongitudeMeasure',
        'AquiferName': 'wqx:WellInformation/wqx:AquiferName',
        'HorizontalAccuracyMeasure/MeasureValue': 'wqx:MonitoringLocationGeospatial/wqx:HorizontalAccuracyMeasure/wqx:MeasureValue',
        'HUCEightDigitCode': 'wqx:MonitoringLocationIdentity/wqx:HUCEightDigitCode',
        'LatitudeMeasure': 'wqx:MonitoringLocationGeospatial/wqx:LatitudeMeasure',
        'ContributingDrainageAreaMeasure/MeasureValue': 'wqx:MonitoringLocationIdentity/wqx:ContributingDrainageAreaMeasure/wqx:MeasureValue',
        'WellDepthMeasure/MeasureUnitCode': 'wqx:WellInformation/wqx:WellDepthMeasure/wqx:MeasureUnitCode',
        'MonitoringLocationIdentifier': 'wqx:MonitoringLocationIdentity/wqx:MonitoringLocationIdentifier',
        'HorizontalCollectionMethodName': 'wqx:MonitoringLocationGeospatial/wqx:HorizontalCollectionMethodName',
        'VerticalAccuracyMeasure/MeasureValue': 'wqx:MonitoringLocationGeospatial/wqx:VerticalAccuracyMeasure/wqx:MeasureValue',
        'VerticalCollectionMethodName': 'wqx:MonitoringLocationGeospatial/wqx:VerticalCollectionMethodName',
        'MonitoringLocationDescriptionText': 'wqx:MonitoringLocationIdentity/wqx:MonitoringLocationDescriptionText',
        'VerticalMeasure/MeasureValue': 'wqx:MonitoringLocationGeospatial/wqx:VerticalMeasure/wqx:MeasureValue',
        'VerticalMeasure/MeasureUnitCode': 'wqx:MonitoringLocationGeospatial/wqx:VerticalMeasure/wqx:MeasureUnitCode',
        'CountyCode': 'wqx:MonitoringLocationGeospatial/wqx:CountyCode',
        'ConstructionDateText': 'wqx:WellInformation/wqx:ConstructionDateText',
        'WellHoleDepthMeasure/MeasureUnitCode': 'wqx:WellInformation/wqx:WellHoleDepthMeasure/wqx:MeasureUnitCode',
        'SourceMapScaleNumeric': 'wqx:MonitoringLocationGeospatial/wqx:SourceMapScaleNumeric'}


    # item-specific (i.e. single-row value) column defs descended from /WQX/Organization/Activity/Result.
    # The column name is the key. The value is the RELATIVE XPath from
    # /WQX/Organization/Activity/Result, obeying the convention that "wqx" is the 
    #  XPath expression's expected alias for the WQX namespace.

    result_col_xpaths = {
        'PrecisionValue': 'wqx:ResultDescription/wqx:DataQuality/wqx:PrecisionValue',
        'ResultAnalyticalMethod/MethodIdentifierContext': 'wqx:ResultAnalyticalMethod/wqx:MethodIdentifierContext',
        'SampleTissueAnatomyName': 'wqx:BiologicalResultDescription/wqx:SampleTissueAnatomyName',
        'StatisticalBaseCode': 'wqx:ResultDescription/wqx:StatisticalBaseCode',
        'ResultWeightBasisText': 'wqx:ResultDescription/wqx:ResultWeightBasisText',
        'ResultMeasure/MeasureUnitCode': 'wqx:ResultDescription/wqx:ResultMeasure/wqx:MeasureUnitCode',
        'ResultDetectionConditionText': 'wqx:ResultDescription/wqx:ResultDetectionConditionText',
        'ResultSampleFractionText': 'wqx:ResultDescription/wqx:ResultSampleFractionText',
        'LaboratoryName': 'wqx:ResultLabInformation/wqx:LaboratoryName',
        'AnalysisStartDate': 'wqx:ResultLabInformation/wqx:AnalysisStartDate',
        'DetectionQuantitationLimitTypeName': 'wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitTypeName',
        'MethodDescriptionText': 'wqx:ResultAnalyticalMethod/wqx:MethodDescriptionText',
        'ResultAnalyticalMethod/MethodIdentifier': 'wqx:ResultAnalyticalMethod/wqx:MethodIdentifier',
        'ResultTemperatureBasisText': 'wqx:ResultDescription/wqx:ResultTemperatureBasisText',
        'ResultParticleSizeBasisText': 'wqx:ResultDescription/wqx:ResultParticleSizeBasisText',
        'USGSPCode': 'wqx:ResultDescription/wqx:USGSPCode',
        'ResultMeasureValue': 'wqx:ResultDescription/wqx:ResultMeasure/wqx:ResultMeasureValue',
        'MeasureQualifierCode': 'wqx:ResultDescription/wqx:ResultMeasure/wqx:MeasureQualifierCode',
        'CharacteristicName': 'wqx:ResultDescription/wqx:CharacteristicName',
        'ResultStatusIdentifier': 'wqx:ResultDescription/wqx:ResultStatusIdentifier',
        'ResultAnalyticalMethod/MethodName': 'wqx:ResultAnalyticalMethod/wqx:MethodName',
        'ResultDepthAltitudeReferencePointText': 'wqx:ResultDescription/wqx:ResultDepthAltitudeReferencePointText',
        'ResultCommentText': 'wqx:ResultDescription/wqx:ResultCommentText',
        'SubjectTaxonomicName': 'wqx:BiologicalResultDescription/wqx:SubjectTaxonomicName',
        'DetectionQuantitationLimitMeasure/MeasureValue': 'wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitMeasure/wqx:MeasureValue',
        'DetectionQuantitationLimitMeasure/MeasureUnitCode': 'wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitMeasure/wqx:MeasureUnitCode',
        'ResultValueTypeName': 'wqx:ResultDescription/wqx:ResultValueTypeName',
        'PreparationStartDate': 'wqx:LabSamplePreparation/wqx:PreparationStartDate',
        'ResultLaboratoryCommentText': 'wqx:ResultLabInformation/wqx:ResultLaboratoryCommentText',
        'ResultDepthHeightMeasure/MeasureValue': 'wqx:ResultDescription/wqx:ResultDepthHeightMeasure/wqx:MeasureValue',
        'ResultDepthHeightMeasure/MeasureUnitCode': 'wqx:ResultDescription/wqx:ResultDepthHeightMeasure/wqx:MeasureUnitCode',
        'ResultTimeBasisText': 'wqx:ResultDescription/wqx:ResultTimeBasisText'}


    # ---------- precompiled XPath query expressions ('nodeq') for retrieving 
    #            Logical Node nodesets:

    # relative expression from root
    # organizations
    orgs_nodeq = etree.XPath('wqx:WQX/wqx:Organization', namespaces=ns)

    # relative expressions from organization node
    # stations
    stations_nodeq = etree.XPath('wqx:MonitoringLocation', namespaces=ns)
    # activities
    activities_nodeq = etree.XPath('wqx:Activity', namespaces=ns)

    # relative expression from activity node
    # results
    results_nodeq = etree.XPath('wqx:Result', namespaces=ns)


    # ---------- dictionaries of precompiled XPath query expressions ('colvalq') 
    #            for retrieving column values (keys are tabular column names):

    # column values scoped to organizations
    org_colvalq = {}
    for colname in self.org_col_xpaths:
        org_colvalq[colname] = etree.XPath(self.org_col_xpaths[colname] + '/text()', namespaces=ns)

    # column values scoped to stations (Monitoring Locations)
    station_colvalq = {}
    for colname in self.station_col_xpaths:
        station_colvalq[colname] = etree.XPath(self.station_col_xpaths[colname] + '/text()', namespaces=ns)

    # column values scoped to activities
    activity_colvalq = {}
    for colname in self.activity_col_xpaths:
        activity_colvalq[colname] = etree.XPath(self.activity_col_xpaths[colname] + '/text()', namespaces=ns)

    # column values scoped to results
    result_colvalq = {}
    for colname in self.result_col_xpaths:
        result_colvalq[colname] = etree.XPath(self.result_col_xpaths[colname] + '/text()', namespaces=ns)


    def empty_station_dataframe(self):
        dataframe = pandas.DataFrame(index=self.station_cols)
        return dataframe

    def empty_result_dataframe(self):
        dataframe = pandas.DataFrame(index=self.result_cols)
        return dataframe


    def make_rowpart(self, node, valq):
        '''
        Applies the colum val XPath query expression "valq" to the
        XML tree node "node" and returns a dictionary whose keys
        are column names and whose values are merges of the text values of all 
        matching nodes. (Note that if there are multiple such values, there
        were multiple sibling nodes with non-empty text. The "merge" is
        a single-space_delimited concatenation.)
        '''
        retval = {}
        # invoke the compiled XPaths
        for colname in valq:
            retval[colname] =  ' '.join(valq[colname](node))
        return retval

    def determine_table_type(self, response):
        '''
        This method uses the response properties to determine what should be
        done.
        If the status code is not a 2xx, it will raise a BaseException.
        If the status code is 2xx, it will inspect the response.url to determine
        whether the resultset should be framed as a Station or Result table
        (or, in future, a Biodata or Simplestation table.) If it cannot 
        determine the correct table type, it will raise a BaseException.
        '''
        if response.status_code < 200 or response.status_code >= 300:
            raise(BaseException('The response is not OK: status code "' + 
                str(response.status_code) + ' ' + response.reason + '".'))

        table_type = ''
        if 'Station/search' in response.url:
            table_type = 'station'
        elif 'Result/search' in response.url:
            table_type = 'result'

        if not table_type:
            raise(BaseException('Unable to determine table type from response URL "'
                + response.url + '".'))
        return table_type


    def xml_to_list_of_dicts(self, root):
        rows = []
        orgs = self.orgs_nodeq(root)
        for org in orgs:
            org_rowpart = self.make_rowpart(org, self.org_colvalq)
            activities = self.activities_nodeq(org)
            for activity in activities:
                activity_rowpart = self.make_rowpart(activity, self.activity_colvalq)
                results = self.results_nodeq(activity)
                for result in results:
                    result_rowpart = self.make_rowpart(result, self.result_colvalq)
                    this_row = {}
                    this_row.update(org_rowpart)
                    this_row.update(activity_rowpart)
                    this_row.update(result_rowpart)
                    rows.append(this_row)
        return rows


    def make_dataframe_from_xml_response(self, response, wqx_namespace_url):
        table_type = self.determine_table_type(response)

        dataframe = None
        if table_type == 'station':
            dataframe = self.empty_station_dataframe()
        elif table_type == 'result':
            dataframe = self.empty_result_dataframe()

        if dataframe and response.content:
            root = et.fromstring(response.content)

            # these repeated elements should be processed once
            wqx_org_nodes = root.findall('.//{' + wqx_namespace_url + '}OrganizationDescription')

            activity_nodes = []
            if table_type == 'result':
                activity_nodes = root.findall('.//{' + wqx_namespace_url + '}Activity')

            row_element = self.ns_row_nodepath(table_type, wqx_namespace_url)[-1]
            row_nodes = root.findall('.//' + row_element)

            print('wqx_org_nodes count: ' + str(len(wqx_org_nodes)))
            print('activity_nodes count: ' + str(len(activity_nodes)))
            print(table_type + ' row_nodes count: ' + str(len(row_nodes)))

            # better to add a single list of dictionaries to the dataframe:
            # it's reportedly agonizingly slow to add each row dict one by one
            datarows = []

            # demo
            org_node = org_nodes[0]
            if activity_nodes:
                activity_node = activity_nodes[0]
            row_node = row_nodes[0]

            org_node_cols = {}
            for colname in self.wqx_organization_cols.keys():
                xpath = self.wqx_organization_cols[colname] + '/text()'
                print(xpath)
                org_node_cols[colname] = org_node.xpath(self.wqx_organization_cols[colname] + '/text()')

            for colname in org_node_cols:
                print('column ' + colname)
                print('\t' + org_node_cols[colname])

            
            row_dict = self.make_row_dict(table_type, root, row_node, org_node, activity_node)
            
        return dataframe


    def make_row_dict(self, table_type, root, row_node, org_node, activity_node):



        retval = {}

        return retval
