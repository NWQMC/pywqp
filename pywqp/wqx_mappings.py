import pandas
from lxml import etree

'''
MAPPING WQX TO ITS CANONICAL TABULAR FORM

The canonical tabular form of WQX data is defined as a sequence of columns.
This representation is used to structure the CSV and TSV downloads, and is
the basis for importing WQX into a pandas dataframe. 

For the purposes of canonical tabular representation, a column 
is considered to map uniquely to a particular semantic data definition.
(For semantic definitions, see 
http://www.waterqualitydata.us/portal_userguide.jsp#WQPUserGuide-Retrieve .)
Since the definitive statement of the data definitions is the WQX schema, the
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

# A formal and complete statement of the mapping between a (semantic) column
# name and the tagnames of XML descendant steps for WQX data. 
# Al paths relevant to /Station/search and /Result/search tabular
# data were derived from this and must be consistent with this.
# All tagnames are in the WQX 2.0 namespace.
#
# For semantic column definitions, see 
# http://www.waterqualitydata.us/portal_userguide.jsp#WQPUserGuide-Retrieve
column_mappings = {
            'ActivityBottomDepthHeightMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityBottomDepthHeightMeasure', 'MeasureUnitCode'],
    'ActivityBottomDepthHeightMeasure/MeasureValue': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityBottomDepthHeightMeasure', 'MeasureValue'],
    'ActivityCommentText': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityCommentText'],
    'ActivityConductingOrganizationText': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityConductingOrganizationText'],
    'ActivityDepthAltitudeReferencePointText': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityDepthAltitudeReferencePointText'],
    'ActivityDepthHeightMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityDepthHeightMeasure', 'MeasureUnitCode'],
    'ActivityDepthHeightMeasure/MeasureValue': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityDepthHeightMeasure', 'MeasureValue'],
    'ActivityEndDate': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityEndDate'],
    'ActivityEndTime/Time': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityEndTime', 'Time'],
    'ActivityEndTime/TimeZoneCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityEndTime', 'TimeZoneCode'],
    'ActivityIdentifier': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityIdentifier'],
    'ActivityMediaName': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityMediaName'],
    'ActivityMediaSubdivisionName': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityMediaSubdivisionName'],
    'ActivityStartDate': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityStartDate'],
    'ActivityStartTime/Time': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityStartTime', 'Time'],
    'ActivityStartTime/TimeZoneCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityStartTime', 'TimeZoneCode'],
    'ActivityTopDepthHeightMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityTopDepthHeightMeasure', 'MeasureUnitCode'],
    'ActivityTopDepthHeightMeasure/MeasureValue': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityTopDepthHeightMeasure', 'MeasureValue'],
    'ActivityTypeCode': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ActivityTypeCode'],
    'AnalysisStartDate': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'AnalysisStartDate'],
    'AquiferName': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'AquiferName'],
    'AquiferTypeName': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'AquiferTypeName'],
    'CharacteristicName': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'CharacteristicName'],
    'ConstructionDateText': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'ConstructionDateText'],
    'ContributingDrainageAreaMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'ContributingDrainageAreaMeasure', 'MeasureUnitCode'],
    'ContributingDrainageAreaMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'ContributingDrainageAreaMeasure', 'MeasureValue'],
    'CountryCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'CountryCode'],
    'CountyCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'CountyCode'],
    'DetectionQuantitationLimitMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'ResultDetectionQuantitationLimit', 'DetectionQuantitationLimitMeasure', 'MeasureUnitCode'],
    'DetectionQuantitationLimitMeasure/MeasureValue': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'ResultDetectionQuantitationLimit', 'DetectionQuantitationLimitMeasure', 'MeasureValue'],
    'DetectionQuantitationLimitTypeName': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'ResultDetectionQuantitationLimit', 'DetectionQuantitationLimitTypeName'],
    'DrainageAreaMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'DrainageAreaMeasure', 'MeasureUnitCode'],
    'DrainageAreaMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'DrainageAreaMeasure', 'MeasureValue'],
    'FormationTypeText': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'FormationTypeText'],
    'HUCEightDigitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'HUCEightDigitCode'],
    'HorizontalAccuracyMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'HorizontalAccuracyMeasure', 'MeasureUnitCode'],
    'HorizontalAccuracyMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'HorizontalAccuracyMeasure', 'MeasureValue'],
    'HorizontalCollectionMethodName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'HorizontalCollectionMethodName'],
    'HorizontalCoordinateReferenceSystemDatumName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'HorizontalCoordinateReferenceSystemDatumName'],
    'HydrologicCondition': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'HydrologicCondition'],
    'HydrologicEvent': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'HydrologicEvent'],
    'LaboratoryName': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'LaboratoryName'],
    'LatitudeMeasure': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'LatitudeMeasure'],
    'LongitudeMeasure': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'LongitudeMeasure'],
    'MeasureQualifierCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultMeasure', 'MeasureQualifierCode'],
    'MethodDescriptionText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultAnalyticalMethod', 'MethodDescriptionText'],
    'MonitoringLocationDescriptionText': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'MonitoringLocationDescriptionText'],
    'MonitoringLocationIdentifier': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'MonitoringLocationIdentifier'],
    'MonitoringLocationName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'MonitoringLocationName'],
    'MonitoringLocationTypeName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationIdentity', 'MonitoringLocationTypeName'],
    'OrganizationFormalName': ['WQX', 'Organization', 'OrganizationDescription', 'OrganizationFormalName'],
    'OrganizationIdentifier': ['WQX', 'Organization', 'OrganizationDescription', 'OrganizationIdentifier'],
    'PrecisionValue': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'DataQuality', 'PrecisionValue'],
    'PreparationStartDate': ['WQX', 'Organization', 'Activity', 'Result', 'LabSamplePreparation', 'PreparationStartDate'],
    'ProjectIdentifier': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'ProjectIdentifier'],
    'ResultAnalyticalMethod/MethodIdentifier': ['WQX', 'Organization', 'Activity', 'Result', 'ResultAnalyticalMethod', 'MethodIdentifier'],
    'ResultAnalyticalMethod/MethodIdentifierContext': ['WQX', 'Organization', 'Activity', 'Result', 'ResultAnalyticalMethod', 'MethodIdentifierContext'],
    'ResultAnalyticalMethod/MethodName': ['WQX', 'Organization', 'Activity', 'Result', 'ResultAnalyticalMethod', 'MethodName'],
    'ResultCommentText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultCommentText'],
    'ResultDepthAltitudeReferencePointText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultDepthAltitudeReferencePointText'],
    'ResultDepthHeightMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultDepthHeightMeasure', 'MeasureUnitCode'],
    'ResultDepthHeightMeasure/MeasureValue': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultDepthHeightMeasure', 'MeasureValue'],
    'ResultDetectionConditionText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultDetectionConditionText'],
    'ResultLaboratoryCommentText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultLabInformation', 'ResultLaboratoryCommentText'],
    'ResultMeasure/MeasureUnitCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultMeasure', 'MeasureUnitCode'],
    'ResultMeasureValue': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultMeasure', 'ResultMeasureValue'],
    'ResultParticleSizeBasisText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultParticleSizeBasisText'],
    'ResultSampleFractionText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultSampleFractionText'],
    'ResultStatusIdentifier': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultStatusIdentifier'],
    'ResultTemperatureBasisText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultTemperatureBasisText'],
    'ResultTimeBasisText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultTimeBasisText'],
    'ResultValueTypeName': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultValueTypeName'],
    'ResultWeightBasisText': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'ResultWeightBasisText'],
    'SampleAquifer': ['WQX', 'Organization', 'Activity', 'ActivityDescription', 'SampleAquifer'],
    'SampleCollectionEquipmentName': ['WQX', 'Organization', 'Activity', 'SampleDescription', 'SampleCollectionEquipmentName'],
    'SampleCollectionMethod/MethodIdentifier': ['WQX', 'Organization', 'Activity', 'SampleDescription', 'SampleCollectionMethod', 'MethodIdentifier'],
    'SampleCollectionMethod/MethodIdentifierContext': ['WQX', 'Organization', 'Activity', 'SampleDescription', 'SampleCollectionMethod', 'MethodIdentifierContext'],
    'SampleCollectionMethod/MethodName': ['WQX', 'Organization', 'Activity', 'SampleDescription', 'SampleCollectionMethod', 'MethodName'],
    'SampleTissueAnatomyName': ['WQX', 'Organization', 'Activity', 'Result', 'BiologicalResultDescription', 'SampleTissueAnatomyName'],
    'SourceMapScaleNumeric': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'SourceMapScaleNumeric'],
    'StateCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'StateCode'],
    'StatisticalBaseCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'StatisticalBaseCode'],
    'SubjectTaxonomicName': ['WQX', 'Organization', 'Activity', 'Result', 'BiologicalResultDescription', 'SubjectTaxonomicName'],
    'USGSPCode': ['WQX', 'Organization', 'Activity', 'Result', 'ResultDescription', 'USGSPCode'],
    'VerticalAccuracyMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalAccuracyMeasure', 'MeasureUnitCode'],
    'VerticalAccuracyMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalAccuracyMeasure', 'MeasureValue'],
    'VerticalCollectionMethodName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalCollectionMethodName'],
    'VerticalCoordinateReferenceSystemDatumName': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalCoordinateReferenceSystemDatumName'],
    'VerticalMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalMeasure', 'MeasureUnitCode'],
    'VerticalMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'MonitoringLocationGeospatial', 'VerticalMeasure', 'MeasureValue'],
    'WellDepthMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'WellDepthMeasure', 'MeasureUnitCode'],
    'WellDepthMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'WellDepthMeasure', 'MeasureValue'],
    'WellHoleDepthMeasure/MeasureUnitCode': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'WellHoleDepthMeasure', 'MeasureUnitCode'],
    'WellHoleDepthMeasure/MeasureValue': ['WQX', 'Organization', 'MonitoringLocation', 'WellInformation', 'WellHoleDepthMeasure', 'MeasureValue']}


# WQX dataset types defined for tabular representation
known_table_types = ('station', 'result')

#tabular definitions as column sequences
tabular_defs = {}

# the ordered column names of the tabular form of a /Station/search dataset
tabular_defs['station'] = (
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
tabular_defs['result'] = (
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


# common (i.e. shared across multiple rows) column defs descended from /WQX/Organization
# Fortunately, these column defs are identical in Station and Result mappings.
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




class WQXMapper:


    # ---------- namespace prefix dictionary
    wqx_namespace_url = 'http://qwwebservices.usgs.gov/schemas/WQX-Outbound/2_0/'
    ns = {'wqx': wqx_namespace_url}


    # ---------- precompiled XPath query expressions ('nodeq') for retrieving 
    #            Logical Node nodesets:

    # relative expression from root
    # organizations
    orgs_nodeq = etree.XPath('/wqx:WQX/wqx:Organization', namespaces=ns)

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

    # column values scoped to an Organization node
    org_colvalq = {}
    for colname in org_col_xpaths:
        org_colvalq[colname] = etree.XPath(org_col_xpaths[colname] + '/text()', namespaces=ns, smart_strings=False)

    # column values scoped to a Station (MonitoringLocation) node
    station_colvalq = {}
    for colname in station_col_xpaths:
        station_colvalq[colname] = etree.XPath(station_col_xpaths[colname] + '/text()', namespaces=ns, smart_strings=False)

    # column values scoped to an Activity node
    activity_colvalq = {}
    for colname in activity_col_xpaths:
        activity_colvalq[colname] = etree.XPath(activity_col_xpaths[colname] + '/text()', namespaces=ns, smart_strings=False)

    # column values scoped to a Result node
    result_colvalq = {}
    for colname in result_col_xpaths:
        result_colvalq[colname] = etree.XPath(result_col_xpaths[colname] + '/text()', namespaces=ns, smart_strings=False)


    def make_rowpart(self, node, valq):
        '''
        Applies the column val XPath query expression "valq" to the
        XML context node "node" and returns a dictionary whose keys
        are column names matching descendants of the context node.
        
        Dictionary values are merges of the text values of all descendant
        nodes that map to the key (Note that if there are multiple such values, 
        there were multiple sibling nodes with non-empty text.) 
        The "merge" is a single-space_delimited concatenation.
        '''
        retval = {}
        # invoke the compiled XPaths
        for colname in valq:
            retval[colname] =  ' '.join(valq[colname](node))
        return retval

    def determine_table_type(self, response):
        '''
        This method inspects the response .status_code and .url properties
        to determine what should be done.
            - If the status code is not a 2xx, it will raise a BaseException.
            - If the status code is 2xx, it will inspect the response.url to 
        determinewhether the resultset should be framed as a Station or 
        Result table (or, in future, a Biodata or Simplestation table.) 
        If it cannot determine the correct table type, it will raise a 
        BaseException.
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

    def xml_to_dict_of_lists(self, table_type, root):
        '''
        Given a known table_type and an XML root node, this method will attempt
        to construct a tabular WQX representation of the information contained
        in the XML.

        The tabular representation is a dictionary whose keys are column names.
        Each column's value is a list of values. The length of each list is
        equal to the number of rows that will be represented in the table.
        The values in a list are determined by the valsq XPath expressions, 
        based on the Logical Nodes in context when the row was evaluated.

        The Lists are all the same length. When the XML does not supply a
        value, an empty string is inserted.

        The number of rows in the tabular representation is equal to the length 
        of any List.

        A single row is determined by slicing all of the Lists at the same 
        index.

        If the table_type is not known, this method returns an empty list.

        If the XML root is not valid WQX, behavior is not specified. This method
        does not attempt XML validation.
        '''
        datadict = {}
        for colname in tabular_defs['result']:
            datadict[colname] = []
        orgs = self.orgs_nodeq(root)
        for org in orgs:
            org_rowpart = self.make_rowpart(org, self.org_colvalq)
            if table_type == 'result':
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
                        for colname in tabular_defs['result']:
                            val = this_row.get(colname)
                            if not val:
                                val = ''
                            datadict[colname].append(val)
            elif table_type == 'station':
                stations = self.stations_nodeq(org)
                for station in stations:
                    station_rowpart = self.make_rowpart(station, self.station_colvalq)
                    this_row = {}
                    this_row.update(org_rowpart)
                    this_row.update(station_rowpart)
                    for colname in tabular_defs['result']:
                        val = this_row.get(colname)
                        if not val:
                            val = ''
                        datadict[colname].append(val)
        return datadict


    def xml_to_list_of_dicts(self, table_type, root):
        '''
        Given a known table_type and an XML root node, this method will attempt
        to construct a tabular WQX representation of the information contained
        in the XML.

        The tabular representation is a list of dictionaries. Each dict in
        the list corresponds to a table row. The dictionary keys are column names,
        and the values are the values extracted from the XML according to the 
        nodeq and valsq XPath expressions in wqx_mappings.

        The number of rows in the tabular representation is equal to the
        length of the returned list.

        A single row is determined by taking a single dict out of the returned List.

        If the table_type is not known, this method returns an empty list.

        If the XML root is not valid WQX, behavior is not specified. This method
        does not attempt XML validation.
        '''
        rows = []
        orgs = self.orgs_nodeq(root)
        for org in orgs:
            org_rowpart = self.make_rowpart(org, self.org_colvalq)
            if table_type == 'result':
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
            elif table_type == 'station':
                stations = self.stations_nodeq(org)
                for station in stations:
                    station_rowpart = self.make_rowpart(station, self.station_colvalq)
                    this_row = {}
                    this_row.update(org_rowpart)
                    this_row.update(station_rowpart)
                    rows.append(this_row)
        return rows


    def make_dataframe_from_xml(self, table_type, root):
        '''
        This method accepts a known table_type and an XML root node. It returns
        a pandas.DataFrame containing the tabular representation of the data
        contained in the "root" argument. The DataFrame return value will
        have columns defined as being equal to the corresponding member of 
        tabular_defs, even if the columns are not populated in any of the records
        embodied in the XML root.

        Expected behavior with respect to improper parameters is similar to 
        that documented for xml_to_list_of_dicts(table_type, root).
        '''
        dataframe = None
        col_defs = tabular_defs[table_type]
            
        if col_defs:
            data_rows = self.xml_to_list_of_dicts(table_type, root)
            dataframe = pandas.DataFrame(data=data_rows, columns=col_defs)
        return dataframe

    
    def make_dataframe_from_xml_response(self, response):
        '''
        This method accepts a requests.response HTTP Response object.
        The assumption is that this response was obtained by calling
        a WQP RESTlike service as described at 
        http://www.waterqualitydata.us/webservices_documentation.jsp.

        This method
          - checks the status code, raising BaseException if not 2xx
          - attempts to identify the table_type, raising BaseException if 
            the response cannot be identified as a known type
          - attempts to parse the XML content, if any
          - attempts to convert the XML content to the correct tabular form
          - returns a pandas.DataFrame containing the tabular data
        '''

        retval = None
        table_type = self.determine_table_type(response)

        if table_type and response.content:
            root = et.fromstring(response.content)
            retval = make_dataframe_from_xml(table_type, root)

        return retval

