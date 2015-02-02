"""Used to wrap normal expanders with deepy specific functionality

most likely use of all of them is to remove all the $(deepy_var)'s to
what they are supposed to be
"""

import deepy.make

import builder.expanders

def deepy_convert_class(expanded_targets):
    """Removes the $(deepy_var)'s from all of the target's ids"""
    for expanded_target in expanded_targets:
        expanded_target.unique_id = deepy.make.subst_deepy_str(
                expanded_target.unique_id)

    return expanded_targets

class DeepyExpander(builder.expanders.Expander):
    """Wraps the expander and converts all of the ids of the
    expanded targets and converts the $(deepy_var)'s
    """
    def expand(self, build_context):
        expanded_targets = super(DeepyExpander, self).expand(build_context)
        return deepy_convert_class(expanded_targets)

class DeepyTimestampExpander(builder.expanders.TimestampExpander):
    """Wraps the timestamp expander and converts all of the ids of the
    expanded targets and converts the $(deepy_var)'s
    """
    def expand(self, build_context):
        expanded_targets = super(
                DeepyTimestampExpander, self).expand(build_context)
        return deepy_convert_class(expanded_targets)
