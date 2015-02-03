import arrow
import time
import re

import deepy.cfg


def basic_command_substitution(fmt_str, timestamp):
    """
    Does the $() (deepy.cfg) subst and timestamp subst.
    These are the substitutions needed to form target and prereqs.
    """
    # '$(cubes_dir)/cube.%Y-%m-%d-%H.h5' -> '$(cubes_dir)/cube.2014-04-28-14.h5'
    if isinstance(timestamp, arrow.arrow.Arrow):
        timestamp = timestamp.float_timestamp
    else:
        timestamp = float(timestamp)
    time_str = time.strftime(fmt_str, time.gmtime(timestamp))

    return time_str

def deepy_command_substitution(fmt_str, config=deepy.cfg):
    """
    Used to replace the $() (deepy.cfg) subst
    """

    cfg_subst_regex = re.compile(r"\$\((?P<deepy_cfg_attr>[^)]+)\)")
    # '$(cubes_dir)/cube.2014-04-28-14.h5' -> '/pipedream/.../cube.2014-04-28-14.h5'
    out_str = fmt_str
    for match in re.finditer(cfg_subst_regex, out_str):
        deepy_cfg_attr = match.group("deepy_cfg_attr")

        if not hasattr(config, deepy_cfg_attr):
            deepy.log.error("deepy-cfg-missing-attribute %s" % (deepy_cfg_attr))
            return None

        config_value = unicode(getattr(config, deepy_cfg_attr))
        out_str = out_str.replace(match.group(), config_value)
    return out_str
