# application's user retention using Scala
A page view event is represented by a timestamp, the url path of
the page visited and a user cookie. Page views made by the same user share the same user
cookie. Each event has been assigned a UUID.

The file has the following fields, in order:
##### event_id => A UUID for each event
##### collector_tstamp =>Time stamp for the event recorded by the collector
##### domain_userid => User ID set in a long-lived browser cookie
##### page_urlpath => Path to page

Data Transformation
Goal is to transform the dataset of individual page view events into a collection of linked
lists (https://en.wikipedia.org/wiki/Linked_list) that represent the journeys that each user took
through the website. In other words, each linked list is the sequence of page views, in
chronological order, made by a single user.

A linked list consists of a set of elements where each element contains a reference for the
element that comes next in the list. In this case, you will use the event_id field to reference a
page view. In order to transform a sequence of page views made by a single user into a linked
list, you must add the ID of each event (as a foreign key) to the event that directly preceded it.
Therefore, your transformation will ultimately add a single field to the original dataset. Write the
transformed dataset to a CSV file with the following fields, in order:

##### event_id => A UUID for each event
##### collector_tstamp => Time stamp for the event recorded by the collector
##### domain_userid => User ID set in a long-lived browser cookie
##### page_urlpath => Path to page
##### next_event_id => The UUID of the next event for the same user (may be null)
