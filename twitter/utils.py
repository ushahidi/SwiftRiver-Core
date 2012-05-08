# Predicate update queue
FIREHOSE_QUEUE = "TWITTER_FIREHOSE_QUEUE"
TWITTER_UPDATE_QUEUE = "TWITTER_UPDATE_QUEUE"

# Maximum no. of track predicates
MAX_TRACK_PREDICATES = 400

# Maximum no. of follow predicates
MAX_FOLLOW_PREDICATES = 5000


def flatten_filter_predicates(predicates):
    """
    Given a dictionary of filter predicates, return a list of tuples
    of predicates and the list of river ids they relate to.
    """
    combined = {}
    for k, v in predicates.iteritems():
        for term, river_ids in v.iteritems():
            try:
                combined[term].update(set(river_ids))
            except KeyError:
                combined[term] = set(river_ids)

    # Final set - generate the tuples
    output = []
    for k, v in combined.iteritems():
        output.append((k, list(v)))

    return output


def allow_filter_predicate(predicates, predicate_key):
    """ Given a dictionary of predicates, determines if
    the maximum allowable number has been reached"""

    # Predicate key does not exist
    if not predicate_key in predicates:
        return True

    # Check for the track predicates (keywords)
    if (predicate_key == 'track'
        and len(predicates[predicate_key]) < MAX_TRACK_PREDICATES):
        return True

    # Check the follow predicates
    if (predicate_key == 'follow'
        and len(predicates[predicate_key]) < MAX_FOLLOW_PREDICATES):
        return True

    # Default
    return False
