# """used to test the abstracted nodes such as the timeexpander and the
# backbone node
# """
#
# import unittest
# import testing
#
# import arrow
# import mock
#
# import deepy.build.node
#
# class TimestampExpandedNodeTest(unittest.TestCase):
#     """Used to test the timestamp expanded node"""
#
#     @testing.unit
#     def test_expand(self):
#         # given
#         node1 = deepy.build.node.TimestampExpandedNodeTester01
#         node2 = deepy.build.node.TimestampExpandedNodeTester02
#
#         node1_time1 = arrow.get("2014-12-05")
#         node1_time2 = arrow.get("2014-12-06")
#
#         node2_time1 = arrow.get("2014-12-05")
#         node2_time2 = arrow.get("2015-03-29")
#
#         node1_build_context = {
#             "start_time": node1_time1,
#             "end_time": node1_time2,
#         }
#
#         node2_build_context = {
#             "start_time": node2_time1,
#             "end_time": node2_time2,
#         }
#
#         expanded_node1 = node1.expand(node1_build_context)
#         expanded_node2 = node2.expand(node2_build_context)
#
#         self.assertEqual(len(expanded_node1), 206)
#         self.assertEqual(len(expanded_node2), 912)
#
#         self.assertEqual(expanded_node1[2].unique_id,
#                 "timestamp_expanded_node_01-2014-12-05-00-09")
#         self.assertEqual(expanded_node2[1].unique_id,
#                 "timestamp_expanded_node_02-2014-12-05-03-00")
#
#     @testing.unit
#     def test_enable(self):
#         # given
#         node1 = deepy.build.node.TimestampExpandedNodeTester01
#         node2 = deepy.build.node.TimestampExpandedNodeTester02
#
#         self.assertFalse(node1.get_enable())
#         self.assertTrue(node2.get_enable())
#
# class BackboneNodeTest(unittest.TestCase):
#     """Used to test that the backbone is enabled correctly"""
#
#     @testing.unit
#     def test_enable(self):
#         # given
#         node1 = deepy.build.node.BackboneNodeTester01
#         node2 = deepy.build.node.BackboneNodeTester02
#
#         # when
#         config = {"has_backbone": False}
#         enable1 = node1.get_enable(config)
#
#         config = {"has_backbone": True}
#         enable2 = node2.get_enable(config)
#
#         # then
#         self.assertFalse(enable1)
#         self.assertTrue(enable2)
#
#     @testing.unit
#     def test_expand(self):
#         # given
#         node1 = deepy.build.node.BackboneNodeTester01
#         node2 = deepy.build.node.BackboneNodeTester02
#
#         build_context = {}
#
#         # when
#         unique_id1 = node1.expand(build_context)[0].unique_id
#
#         config = {"has_backbone": True}
#         unique_id2 = node2.expand(build_context, config=config)[0].unique_id
#
#         # then
#         self.assertEqual(unique_id1, "backbone_node_01resolved")
#         self.assertEqual(unique_id2, "backbone_node_02True")
