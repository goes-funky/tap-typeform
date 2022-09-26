# REFERENCE

## Description

Typeform integration can offer us retrieving data for answers, forms, landing and questions table.
For a complete overview, refer to [Typeform API]({https://developer.typeform.com/get-started/})

***


## Supported Features

| **Feature Name**                                                                        | **Supported** | **Comment** |
| --------------------------------------------------------------------------------------- | ------------- | ----------- |
| [Full Import](https://docs.y42.com/docs/features#full-import)                           | Yes        |             |
| [Partial Import](https://docs.y42.com/docs/features#partial-import)                     | No        |             |
| [Re-Sync](https://docs.y42.com/docs/features#re-sync)                                   | No        |             |
| [Start Date Selection](https://docs.y42.com/docs/features#start-date-selection)         | Yes        |             |
| [Import Empty Tables](https://docs.y42.com/docs/features#import-empty-table)            | No        |             |
| [Custom Data](https://docs.y42.com/docs/features#custom-data)                           | No        |             |
| [Retroactive Updating](https://docs.y42.com/docs/features#retroactive-updating)         | No        |             |
| [Dynamic Column Selection](https://docs.y42.com/docs/features#dynamic-column-selection) | Yes        |             |

***


## Sync Overview

###Performance Consideration

API rate limits (2 requests per second): [rate limits](https://developer.typeform.com/get-started/#rate-limits)

###Edge cases or known limitations

Typeform API page size limit per source:
* Forms - 200
* Responses - 1000

Connector performs additional API call to fetch all possible form ids on an account using [retrieve forms endpoint](https://developer.typeform.com/create/reference/retrieve-forms/)

###Recommendations

- Question Data: The form definitions are quite robust, but we have chosen to limit the fields to just those needed for responses analysis.

- Form Data: The raw response data is not fully normalized and the tap output reflects this by breaking it into landings and answers.  Answers could potentially be normalized further, but the redundant data is quite small so it seemed better to keep it flat.  The hidden field was left a JSON structure since it could have any sorts or numbers of custom elements.  

- Timestamps: All timestamp columns are in yyyy-MM-ddTHH:mm:ssZ format.  Resume_date state parameter are Unix timestamps.

***


## Connector 

This connector was developed following the standards of Singer SDK.  
For more details, see the [Singer]({https://singer.io})

### Authentication

Authorisation with token.

### Workflow

- Pulls raw data from TypeForms's [API](https://api.typeform.com/forms)
- Extracts the following resources from TypeForm
  - [Responses](https://developer.typeform.com/responses)
      - List of questions on each form added to the configuration in the tap.
      - List of landings of users onto each form added to the configuration in the tap.
      - List of answers completed during each landing onto each form added to the configuration in the tap.
- Outputs the schema for each resource
- Inside one connector we found multiple pages.

### Rate limits & Pagination

-You can send two requests per second, per type form account. Refer [rate limit]({https://developer.typeform.com/get-started/#rate-limits})
- For pagination should consider that we can retrieve 200 Forms per page  and 1000 Responses per page.

***


## Schema

This source is based on the [Typeform API]({https://developer.typeform.com/}).

### Supported Streams

| **Name**                                                                | **Description** | **Stream Type**                                                                  |
| ----------------------------------------------------------------------- | --------------- | -------------------------------------------------------------------------------- |
| [Forms](https://developer.typeform.com/create/reference/retrieve-form/)       |    Retrieves a form by the given form_id.             | [Typeform API](https://developer.typeform.com/)                  |
| [Answers](https://developer.typeform.com/responses/reference/retrieve-responses/) | A list of form answers with ids that can be used to link to landings and questions since the last completed run of the integration) through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.| [Typeform API](https://developer.typeform.com/)  |
| [Landing](https://developer.typeform.com/responses/reference/retrieve-responses/)         |    A list of form landings and supporting data since the last completed run of the tap through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.| [Typeform API](https://developer.typeform.com/)                  |
| [Questions](https://developer.typeform.com/responses/reference/retrieve-responses/)        |   A list of question titles and ids that can then be used to link to answers.| [Typeform API](https://developer.typeform.com/)                  |

***

#HOW TO SETUP

## Resources

The resources you need to be able to connect to this connector are:

- Date for sync
- Access token

***


## Connector Setup

### How to Get API token 

To get the API token for your application follow this [steps]({https://developer.typeform.com/get-started/personal-access-token/})
* Log in to your account at Typeform.
* In the upper-right corner, in the drop-down menu next to your profile photo, click My Account.
* In the left menu, click Personal tokens.
* Click Generate a new token.
* In the Token name field, type a name for the token to help you identify it.
* Choose needed scopes (API actions this token can perform - or permissions it has). See here for more details on scopes.
* Click Generate token.

### How to Create a new Connector

1. On Integrate, click on "Add..." to search for Typeform and select it.
2. Name your integration.
3. To authorize fill the `Sync Historical Data from` and `API Key` next click Authorize.
4. After authentication, you are good to go and start importing your tables.
