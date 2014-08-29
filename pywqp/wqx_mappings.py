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

A NODESET XPath expression selects Context Nodes:

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


# namespace prefix dictionary
ns = {'wqx': 'http://qwwebservices.usgs.gov/schemas/WQX-Outbound/2_0/'}


# Absolute XPath expressions defining context nodes. "Context nodes"
# are nodes that define certain structural patterns in the XML tree:
# they contain value child nodes, and possibly other context nodes.
# 
# Each kind of tabular definition is associated with a context "leaf" node: one
# that has no context node descendants (or whose context node descendants
# are ignored according to the specification of the tabular definition.) 
# The corresponding tabular row is assembled from the value nodes of 
# the leaf node, and the value nodes of its context ancestors.
context_descriptors = {
    'org':      '/wqx:WQX/wqx:Organization',
    'station':  '/wqx:WQX/wqx:Organization/wqx:MonitoringLocation',
    'activity': '/wqx:WQX/wqx:Organization/wqx:Activity',
    'result':   '/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result'}


# It's logically efficient to express kinds of context nodes that
# cannot be ancestors of a particular kind of context node.
# (we skip 'activity' because it doesn't correspond to a table type.)
context_descriptor_exclusions = {
    'station': ('activity', 'result'),
    'result': ('station',)
}


# Derived XPath expressions for obtaining a set of child context nodes
# from a given useful context.
#
# The Org context nodes are absolute XPath expressions: every record in an
# Outbound WQX document belongs to an Organization.
#
# Station andAactivity nodes are relative expressions from within the context 
# of a given org node.
#
# Result nodes are relative expressions from within the context of a
# given activity node.
context_xpaths = {
    'org': '/wqx:WQX/wqx:Organization',
    'station': 'wqx:MonitoringLocation',
    'activity': 'wqx:Activity',
    'result': 'wqx:Result'}


# Semantic column definitions assigned to absolute position XPaths within a WQX
# XML document. 
#
# Note that it's rare, but possible, for a given Column Definition to occur in more
# than one location. For example, "MonitoringLocationIdentifier" is the result of
# two separate XPath selectors: one that applies to "station" data, and one
# that applies to "result" data. The two are distinguished by inspecting the
# ancestor paths for the occurrence of "station" and "result" context_mapping
# entries respectively.
#
# For semantic column definitions, see 
# http://www.waterqualitydata.us/portal_userguide.jsp#WQPUserGuide-Retrieve

column_mappings = {
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:MonitoringLocationIdentifier": "MonitoringLocationIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityBottomDepthHeightMeasure/wqx:MeasureUnitCode": "ActivityBottomDepthHeightMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityBottomDepthHeightMeasure/wqx:MeasureValue": "ActivityBottomDepthHeightMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityCommentText": "ActivityCommentText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityConductingOrganizationText": "ActivityConductingOrganizationText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityDepthAltitudeReferencePointText": "ActivityDepthAltitudeReferencePointText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityDepthHeightMeasure/wqx:MeasureUnitCode": "ActivityDepthHeightMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityDepthHeightMeasure/wqx:MeasureValue": "ActivityDepthHeightMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityEndDate": "ActivityEndDate",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityEndTime/wqx:Time": "ActivityEndTime/Time",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityEndTime/wqx:TimeZoneCode": "ActivityEndTime/TimeZoneCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityIdentifier": "ActivityIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityMediaName": "ActivityMediaName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityMediaSubdivisionName": "ActivityMediaSubdivisionName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityStartDate": "ActivityStartDate",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityStartTime/wqx:Time": "ActivityStartTime/Time",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityStartTime/wqx:TimeZoneCode": "ActivityStartTime/TimeZoneCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityTopDepthHeightMeasure/wqx:MeasureUnitCode": "ActivityTopDepthHeightMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityTopDepthHeightMeasure/wqx:MeasureValue": "ActivityTopDepthHeightMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ActivityTypeCode": "ActivityTypeCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:HydrologicCondition": "HydrologicCondition",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:HydrologicEvent": "HydrologicEvent",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:MonitoringLocationIdentifier": "MonitoringLocationIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:ProjectIdentifier": "ProjectIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:ActivityDescription/wqx:SampleAquifer": "SampleAquifer",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:BiologicalResultDescription/wqx:SampleTissueAnatomyName": "SampleTissueAnatomyName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:BiologicalResultDescription/wqx:SubjectTaxonomicName": "SubjectTaxonomicName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:LabSamplePreparation/wqx:PreparationStartDate": "PreparationStartDate",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultAnalyticalMethod/wqx:MethodDescriptionText": "MethodDescriptionText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultAnalyticalMethod/wqx:MethodIdentifier": "ResultAnalyticalMethod/MethodIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultAnalyticalMethod/wqx:MethodIdentifierContext": "ResultAnalyticalMethod/MethodIdentifierContext",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultAnalyticalMethod/wqx:MethodName": "ResultAnalyticalMethod/MethodName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:CharacteristicName": "CharacteristicName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:DataQuality/wqx:PrecisionValue": "PrecisionValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultCommentText": "ResultCommentText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultDepthAltitudeReferencePointText": "ResultDepthAltitudeReferencePointText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultDepthHeightMeasure/wqx:MeasureUnitCode": "ResultDepthHeightMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultDepthHeightMeasure/wqx:MeasureValue": "ResultDepthHeightMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultDetectionConditionText": "ResultDetectionConditionText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultMeasure/wqx:MeasureQualifierCode": "MeasureQualifierCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultMeasure/wqx:MeasureUnitCode": "ResultMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultMeasure/wqx:ResultMeasureValue": "ResultMeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultParticleSizeBasisText": "ResultParticleSizeBasisText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultSampleFractionText": "ResultSampleFractionText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultStatusIdentifier": "ResultStatusIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultTemperatureBasisText": "ResultTemperatureBasisText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultTimeBasisText": "ResultTimeBasisText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultValueTypeName": "ResultValueTypeName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:ResultWeightBasisText": "ResultWeightBasisText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:StatisticalBaseCode": "StatisticalBaseCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultDescription/wqx:USGSPCode": "USGSPCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:AnalysisStartDate": "AnalysisStartDate",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:LaboratoryName": "LaboratoryName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitMeasure/wqx:MeasureUnitCode": "DetectionQuantitationLimitMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitMeasure/wqx:MeasureValue": "DetectionQuantitationLimitMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:ResultDetectionQuantitationLimit/wqx:DetectionQuantitationLimitTypeName": "DetectionQuantitationLimitTypeName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:Result/wqx:ResultLabInformation/wqx:ResultLaboratoryCommentText": "ResultLaboratoryCommentText",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:SampleDescription/wqx:SampleCollectionEquipmentName": "SampleCollectionEquipmentName",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodIdentifier": "SampleCollectionMethod/MethodIdentifier",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodIdentifierContext": "SampleCollectionMethod/MethodIdentifierContext",
    "/wqx:WQX/wqx:Organization/wqx:Activity/wqx:SampleDescription/wqx:SampleCollectionMethod/wqx:MethodName": "SampleCollectionMethod/MethodName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:CountryCode": "CountryCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:CountyCode": "CountyCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:HorizontalAccuracyMeasure/wqx:MeasureUnitCode": "HorizontalAccuracyMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:HorizontalAccuracyMeasure/wqx:MeasureValue": "HorizontalAccuracyMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:HorizontalCollectionMethodName": "HorizontalCollectionMethodName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:HorizontalCoordinateReferenceSystemDatumName": "HorizontalCoordinateReferenceSystemDatumName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:LatitudeMeasure": "LatitudeMeasure",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:LongitudeMeasure": "LongitudeMeasure",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:SourceMapScaleNumeric": "SourceMapScaleNumeric",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:StateCode": "StateCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalAccuracyMeasure/wqx:MeasureUnitCode": "VerticalAccuracyMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalAccuracyMeasure/wqx:MeasureValue": "VerticalAccuracyMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalCollectionMethodName": "VerticalCollectionMethodName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalCoordinateReferenceSystemDatumName": "VerticalCoordinateReferenceSystemDatumName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalMeasure/wqx:MeasureUnitCode": "VerticalMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationGeospatial/wqx:VerticalMeasure/wqx:MeasureValue": "VerticalMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:ContributingDrainageAreaMeasure/wqx:MeasureUnitCode": "ContributingDrainageAreaMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:ContributingDrainageAreaMeasure/wqx:MeasureValue": "ContributingDrainageAreaMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:DrainageAreaMeasure/wqx:MeasureUnitCode": "DrainageAreaMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:DrainageAreaMeasure/wqx:MeasureValue": "DrainageAreaMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:HUCEightDigitCode": "HUCEightDigitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:MonitoringLocationDescriptionText": "MonitoringLocationDescriptionText",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:MonitoringLocationName": "MonitoringLocationName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:MonitoringLocationIdentity/wqx:MonitoringLocationTypeName": "MonitoringLocationTypeName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:AquiferName": "AquiferName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:AquiferTypeName": "AquiferTypeName",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:ConstructionDateText": "ConstructionDateText",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:FormationTypeText": "FormationTypeText",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:WellDepthMeasure/wqx:MeasureUnitCode": "WellDepthMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:WellDepthMeasure/wqx:MeasureValue": "WellDepthMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:WellHoleDepthMeasure/wqx:MeasureUnitCode": "WellHoleDepthMeasure/MeasureUnitCode",
    "/wqx:WQX/wqx:Organization/wqx:MonitoringLocation/wqx:WellInformation/wqx:WellHoleDepthMeasure/wqx:MeasureValue": "WellHoleDepthMeasure/MeasureValue",
    "/wqx:WQX/wqx:Organization/wqx:OrganizationDescription/wqx:OrganizationFormalName": "OrganizationFormalName",
    "/wqx:WQX/wqx:Organization/wqx:OrganizationDescription/wqx:OrganizationIdentifier": "OrganizationIdentifier"}


# Tabular definitions as column sequences. The keys of this data structure
# are consistent with the scheme of context nodes provided in context_mappings.
tabular_defs = {}

# the ordered set of column names (as defined in the values of column_mappings) of the 
# tabular form of a Water Quality Portal /Station/search dataset
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

# the ordered column names (as defined in column_mappings) of the 
# tabular form of a Water Quality Portal /Result/search dataset
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


# Value expressions derived from, and consistent with, column_mappings. In this
# dict, the sense of column_mappings's key:value relationship is reversed; 
# ambiguities are resolved by the process of obtaining context nodes.
val_xpaths = {}

# common (i.e. shared across multiple rows) column defs descended from /WQX/Organization
# Fortunately, these column defs are identical in Station and Result mappings.
# The column name is the key. The value is the RELATIVE XPath from
# /WQX/Organization, obeying the convention that "wqx" is the 
#  XPath expression's expected alias for the WQX namespace.
val_xpaths['org'] = {
    'OrganizationIdentifier': 'wqx:OrganizationDescription/wqx:OrganizationIdentifier',
    'OrganizationFormalName': 'wqx:OrganizationDescription/wqx:OrganizationFormalName'}

# common (i.e. shared across multiple rows) column defs descended from /WQX/Organization/Activity.
# These column defs apply to result mappings and not to stations.
# The column name is the key. The value is the RELATIVE path from
# /WQX/Organization/Activity, obeying the convention that "wqx" is the 
#  XPath expression's expected alias for the WQX namespace.
val_xpaths['activity'] = {
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
# # These column defs apply to station mappings and not to results.
# The column name is the key. The value is the RELATIVE XPath from
# /WQX/Organization/MonitoringLocation, obeying the convention that "wqx" is the 
#  XPath expression's expected alias for the WQX namespace.
val_xpaths['station'] = {'DrainageAreaMeasure/MeasureUnitCode': 'wqx:MonitoringLocationIdentity/wqx:DrainageAreaMeasure/wqx:MeasureUnitCode',
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

val_xpaths['result'] = {
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

    # ---------- dictionary of precompiled XPath query expressions
    #            for retrieving logical context nodes (matches
    #            wqx_mappings.context_xpaths):
    context_xpaths_compl = {}
    for nodename in context_xpaths:
        context_xpaths_compl[nodename] = etree.XPath(context_xpaths[nodename], namespaces=ns)
    
    # ---------- precompiled XPath query expressions ('nodeq') for retrieving 
    #            Logical Node nodesets:

    # relative expression from root
    # organizations
    context_xpaths_compl['org'] = etree.XPath('/wqx:WQX/wqx:Organization', namespaces=ns)

    # relative expressions from organization node
    # stations
    context_xpaths_compl['station'] = etree.XPath('wqx:MonitoringLocation', namespaces=ns)
    # activities
    context_xpaths_compl['activity'] = etree.XPath('wqx:Activity', namespaces=ns)

    # relative expression from activity node
    # results
    context_xpaths_compl['result'] = etree.XPath('wqx:Result', namespaces=ns)


    # ---------- dictionaries of precompiled XPath query expressions  
    #            for retrieving column values (keys are tabular column names):
    val_xpaths_compl = {}
    for node in context_xpaths_compl.keys():
        print('doing node \'' + node + '\'')
        val_xpaths_compl[node] = {}
        for colname in val_xpaths[node].keys():
            cur_node_dict = val_xpaths[node]
            cur_xpath = etree.XPath(cur_node_dict[colname] + '/text()', namespaces=ns, smart_strings=False)
            val_xpaths_compl[node][colname] = cur_xpath


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

        The tabular representation is "column-first": a dictionary whose keys 
        are column names.

        Each column's value is a list of values. The length of each list is
        equal to the number of rows that will be represented in the table.
        The values in a list are determined by the val XPath expressions, 
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
        for colname in tabular_defs[table_type]:
            datadict[colname] = []
        orgs = self.context_xpaths_compl['org'](root)
        for org in orgs:
            org_rowpart = self.make_rowpart(org, self.val_xpaths_compl['org'])
            if table_type == 'result':
                activities = self.context_xpaths_compl['activity'](org)
                for activity in activities:
                    activity_rowpart = self.make_rowpart(activity, self.val_xpaths_compl['activity'])
                    results = self.context_xpaths_compl['result'](activity)
                    for result in results:
                        result_rowpart = self.make_rowpart(result, self.val_xpaths_compl['result'])
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
                stations = self.context_xpaths_compl['station'](org)
                for station in stations:
                    station_rowpart = self.make_rowpart(station, self.val_xpaths_compl['station'])
                    this_row = {}
                    this_row.update(org_rowpart)
                    this_row.update(station_rowpart)
                    for colname in tabular_defs['station']:
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

        The tabular representation is "row-first":  a list of dictionaries. 
        Each dict in the list corresponds to a table row. The dictionary keys are 
        column names, and the values are the values extracted from the XML according 
        to the context and val XPath expressions in wqx_mappings.

        The number of rows in the tabular representation is equal to the
        length of the returned list.

        A single row is determined by taking a single dict out of the returned List.

        If the table_type is not known, this method returns an empty list.

        If the XML root is not valid WQX, behavior is not specified. This method
        does not attempt XML validation.
        '''
        rows = []
        orgs = self.context_xpaths_compl['org'](root)
        for org in orgs:
            org_rowpart = self.make_rowpart(org, self.val_xpaths_compl['org'])
            if table_type == 'result':
                activities = self.context_xpaths_compl['activity'](org)
                for activity in activities:
                    activity_rowpart = self.make_rowpart(activity, self.val_xpaths_compl['activity'])
                    results = self.context_xpaths_compl['result'](activity)
                    for result in results:
                        result_rowpart = self.make_rowpart(result, self.val_xpaths_compl['result'])
                        this_row = {}
                        this_row.update(org_rowpart)
                        this_row.update(activity_rowpart)
                        this_row.update(result_rowpart)
                        rows.append(this_row)
            elif table_type == 'station':
                stations = self.context_xpaths_compl['station'](org)
                for station in stations:
                    station_rowpart = self.make_rowpart(station, self.val_xpaths_compl['station'])
                    this_row = {}
                    this_row.update(org_rowpart)
                    this_row.update(station_rowpart)
                    rows.append(this_row)
        return rows


    def make_dataframe_from_xml(self, table_type, root, columns_first=True):
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
            if columns_first:
                data_rows = self.xml_to_dict_of_lists(table_type, root)
            else:
                data_rows = self.xml_to_list_of_dicts(table_type, root)
            dataframe = pandas.DataFrame(data=data_rows, columns=col_defs)
        return dataframe


    def make_dataframe_from_http_response(self, response, columns_first=True):
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
            root = etree.fromstring(response.content)
            retval = self.make_dataframe_from_xml(table_type, root, columns_first)

        return retval

