import sys
import requests

general_args = {}
general_args['paramfile'] = 'a URL or local filesystem reference to a parameter list'
general_args['wqpResourceType'] = '''a REQUIRED parameter with the following valid values:
        - station
        - result
        - simple_station
        - biological_result'''
general_args['providers'] = 'an OPTIONAL (default = all) space-delimited list of providers to which the query will be restricted'

geo_args = {}
geo_args['NOTE'] = 'all latitude and longitude values are expressed decimally as per WGS84.'
geo_args['bBox'] = 'a bounding box whose top and bottom are geographical parallels of latitude, and whose sides are meridians of longitude.'
circle_def = "three arguments taken together represent a circle on the Earth's surface. 'lat and lon' define the circle's center; 'within' is the circle's radius expressed in decimal miles."
geo_args['lat'] = circle_def
geo_args['lon'] = circle_def
geo_args['within'] = circle_def

political_args = {}
political_args['NOTE'] = 'All codes are based on the US FIPS Publications.'
political_args['countrycode'] = ''
political_args['statecode'] = ''
political_args['countycode'] = ''

site_args = {}
site_args['organizationId'] = 'ID of the administrative subunit responsible for maintenance and sampling activities at the site'
site_args['siteId'] = "the site identifier used by the owning organization, appended to the organization's designation, with a single hyphen discriminator."
site_args['siteType'] = ''
site_args['huc'] = 'Hydrologic Unit Code as maintained by USGS'

sampling_args = {}
sampling_args['NOTE'] = '"startDate" means the nominal date of a particular collection effort, which is applied to all samples obtained by that effort. It does not necessarily state the date on which a particular measurement is actually obtained.'
sampling_args['activityId'] = 'the project or program indicating a specific sampling effort'
sampling_args['startDateLo'] = 'the minimum startDate, inclusive, for sampling results'
sampling_args['startDateHi'] = 'the maximum startDate, inclusive, for sampling results'
sampling_args['sampleMedia'] = ''
sampling_args['characteristicType'] = 'a general category applied to sampled characteristics'
sampling_args['characteristicName'] = 'an identifier for a specific sampled characteristic'
sampling_args['pCode'] = 'an NWIS-only classification scheme for sampling procedures. Non-NWIS data sources will not provide any results to a query with a pCode parameter.'
sampling_args['analyticalMethod'] = 'a classification of analytical protocols curated by NEMI (see [http://www.nemi.gov] for more information.) The value of this parameter is a published NEMI URI, _fully urlencoded_.'

known_argdefs = (general_args, geo_args, political_args, site_args, sampling_args)
all_arg_defs = known_argdefs[0].copy()
for argdef in known_argdefs[1:]:
    all_arg_defs.update(argdef)
# NOTE is a special item - it's not actually a parameter name.
all_arg_defs.pop('NOTE', None)



class WQPValidator():

    def param_names(self):
        return all_arg_defs.keys()

    def general_paramdefs(self):
        return general_args.copy()

    def geo_constraint_paramdefs(self):
        return geo_args.copy()

    def political_constraint_paramdefs(self):
        return political_args.copy()

    def site_constraint_paramdefs(self):
        return site_args.copy()

    def sample_constraint_paramdefs(self):
        return sample_args.copy()


    def valid_params(self, parameters):
        return True

    def known_param(self, paramname):
        for paramtype in known_argdefs:
            if paramname in paramtype:
                return paramtype
        return False

    # accepts string <name>=<value>, returns single-entry dict if valid,
    # False if not valid
    def param_from_expr(self, paramexpr):
        parts = paramexpr.split('=')
        #if len(parts) == 2:
            # might be valid

    def param_from_list(self, name_val):
        paramtype = self.known_param(name_val[0])


    def valid_bbox(self, bbox_value):
        return False

    def valid_circle(self, lat_value, lon_value, within_value):
        return False

    def valid_countrycode(self, countrycode_value):
        return False

    def valid_statecode(self, countrycode_values, statecode_value):
        return False

    def valid_countycode(self, statecode_values, countycode_value):
        return False

    def valid_site_type(self, site_type_value):
        return False

    def valid_organization_id(self, organization_id_value):
        return False

    def valid_site_id(self, site_id_value):
        return False

    def valid_huc(self, huc_value):
        return False

    def valid_sample_media(self, sample_media_value):
        return False

    def valid_date_range(self, start_date_lo_value, start_date_hi_value):
        return False

    def valid_characteristic_type(self, characteristic_type_value):
        return False

    def valid_characteristic_name(self, characteristic_name_value):
        return False

    def valid_nwis_parameter_code(self, parameter_code_value):
        return False

