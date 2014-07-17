Feature: Query Water Quality Portal
    In order to obtain Water Quality data
    As a data consumer with some knowledge of Python
    I want to be able to use a general Water Quality download capability

    Scenario: Calumet County WI sites pH HEAD
	Given WQPServer = "http://waterqualitydata.us"
	And countrycode = "US"
	And statecode = "US:55"
	And countycode = "US:55:015"
	And characteristicName = "pH"
	And I want to search for "station" data
	When I query WQP with a "HEAD" request
	Then I should receive a "200" status
	And I should see a "wqp-job-id" header
	And I should see a total-"site-count" greater than 0
	And total-"site-count" should equal the sum of all contributing counts

    Scenario: Calumet County WI sites ph GET
	Given WQPServer = "http://waterqualitydata.us"
	And countrycode = "US"
	And statecode = "US:55"
	And countycode = "US:55:015"
	And characteristicName = "pH"
	And I want to search for "station" data
	And I want it as "text/csv"
	When I query WQP with a "GET" request
	Then I should receive a "200" status
	And I should see a total-"site-count" greater than 0
	And the messagebody should contain as many data rows as the total-site-count reported in the header

    Scenario: Calumet County WI results pH HEAD
	Given WQPServer = "http://waterqualitydata.us"
	And countrycode = "US"
	And statecode = "US:55"
	And countycode = "US:55:015"
	And characteristicName = "pH"
	And I want to search for "result" data
	And I want it as "text/csv"
	When I query WQP with a "HEAD" request
	Then I should receive a "200" status
	And I should see a total-"result-count" greater than 0
	And total-"result-count" should equal the sum of all contributing counts
