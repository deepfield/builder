import argcomplete
import argparse

import builder.deepy_build

from deepy.make import construct_rules
import deepy.log
import deepy.cfg

def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(dest='rule_name', nargs='?') # do not add rule completer before deepy.cfg.init
    p.add_argument('-d', dest='deployment_id')
    p.add_argument('-l', dest='log_cfg')
    p.add_argument('-L', dest='force_local', action='store_true')
    p.add_argument('--rule_names', action='store_true')
    p.add_argument('--disable', choices=['t', 'm', 'tm'], default='')
    p.add_argument('-v', dest='verbose', action='store_true')
    return p

def parse_args():
    parser = get_parser()

    argcomplete.autocomplete(parser)

    return parser.parse_args()

def main():
    import deepy.scheduler

    args = parse_args()
    if args.deployment_id:
        deepy.cfg.init(args.deployment_id)
    if args.log_cfg:
        deepy.log.init(level=args.log_cfg)
    if args.force_local:
        deepy.cfg.force_remote = 'local'

    rules = construct_rules()
    build_graph = builder.deepy_build.DeepyBuild(rules)
    rule_dependency_graph = build_graph.construct_rule_dependency_graph()

    if args.rule_names:
        print '\n'.join(sorted(map(lambda x: x.unexpanded_id, rule_dependency_graph.jobs)))
    elif args.rule_name is not None:
        job = rule_dependency_graph.get_job()
        print job
    else:
        jobs = sorted(rule_dependency_graph.jobs, key=lambda x: x.unexpanded_id)
        for job in jobs:
            print '  %s' % (job.unexpanded_id)
            if args.verbose:
                print job
                print