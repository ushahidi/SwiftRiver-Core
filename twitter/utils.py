# Predicate update queue
FIREHOSE_QUEUE = "TWITTER_FIREHOSE_QUEUE"
TWITTER_UPDATE_QUEUE = "TWITTER_UPDATE_QUEUE"

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