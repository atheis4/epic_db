epic_db
===============================================================================
- Author: Logan Sandar, Andrew Theis, Ben Miltz
- Email: mlsandar@uw.edu, atheis@uw.edu, benmiltz@uw.edu
- Description: interface for creating/modifying/deleting epic_db


**ABOUT**
===============================================================================
The epic_db package represents a sqlalchemy ORM for inserting, modifying, and deleting rows from the epic database. The epic database tracks the inputs to the Epi(demiology) Computation process.

This database allowed for a first at IHME, *sequela hierarchies*.


**USE**
===============================================================================
All updates to the database are processed through the RequestHandler object. This object must be instantiated with a sqlalchemy.orm.Session object pointing to the epic database. 

The RequestHandler processes "requests", nested python dictionaries, via the process_request method. A request dictionary must be structured with downstream dependent table changes nested inside of the upstream table changes (their foreign keys). The RequestHandler will process these requests recursively and throw an error if the request dictionary is not properly configured.

Hierarchy changes can also be made by passing a list of sequela ids through a child field in the request dictionary.
