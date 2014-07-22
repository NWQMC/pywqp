Feature: Stash WQP downloads on disk
    In order to save WQP data for future use
    As a data consumer with some knowledge of Python
    I want to download a file of WQP data and store it on disk

    Scenario: Stash CSV data
	Given that I have downloaded WQP data
	When I stash that data to disk using pywqp
	Then I should see the file on disk with the same byte size as the downloaded file

    Scenario: Verify stashed CSV data
	Given that I have downloaded WQP data
	And I have stashed that data on disk using pywqp
	And I have retained a copy in memory
	When I read the data from disk
	Then the two CSV files should contain the same number of rows
	
    Scenario: Stash entire message
	Given that I have downloaded WQP data
	When I stash the entire message to disk using pywqp
	Then I should see the filepath on disk with appended http suffix

    Scenario: Verify stashed CSV data with http
	Given that I have downloaded WQP data
	And I have stashed the message on disk using pywqp
	And I have retained a copy in memory
	When I read the data from disk
	Then the status line should be present
	And the headers should be present
	And there should be a blank line before the messagebody 
