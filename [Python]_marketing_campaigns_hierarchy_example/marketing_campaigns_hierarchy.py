class Source(object):
    def __init__(self, structure, key, value, list_of_sources = []):
        print('Key:', key)
        print('Value:', value)
        self.list_of_index_elements = [index_ for index_, element_ in enumerate(list(structure.values())[list(structure.keys()).index(key)]) if element_ == value]
        for ind_, elem_ in enumerate(self.list_of_index_elements):
            list_of_sources.append(structure['source'][elem_])
        print('Sources: {}'.format(str(list_of_sources)))

class Medium(Source):
    def __init__(self, structure, key, value, list_of_mediums = []):
        super().__init__(structure, key, value)
        for ind_, elem_ in enumerate(self.list_of_index_elements):
            list_of_mediums.append(structure['medium'][elem_])
        print('Mediums: {}'.format(str(list_of_mediums)))


class Campaign(Medium):
    def __init__(self, structure, key, value, list_campaigns = []):
        super().__init__(structure, key, value)
        for ind_, elem_ in enumerate(self.list_of_index_elements):
            list_campaigns.append(structure['campaign'][elem_])
        print('Campaign Ids: {}'.format(str(list_campaigns)))


class Adgroup(Campaign):
    def __init__(self, structure, key, value, list_of_adgroups = []):
        super().__init__(structure, key, value)
        for ind_, elem_ in enumerate(self.list_of_index_elements):
            list_of_adgroups.append(structure['ad_group_id'][elem_])
        print('Ad Group Ids: {}'.format(str(list_of_adgroups)))

class AdId(Adgroup):
    def __init__(self, structure, key, value, list_of_adids = []):
        super().__init__(structure, key, value)
        for ind_, elem_ in enumerate(self.list_of_index_elements):
            list_of_adids.append(structure['ad_id'][elem_])
        print('Ad_ids: {}'.format(str(list_of_adids)))



structure_dict = {'source': ['google', 'google', 'facebook', 'youtube'],
        'medium': ['cpc', 'cpc', 'cpa', 'cpc'],
        'campaign': ['111', '222', '233', 'aaa'],
        'ad_group_id': ['3333', '4444', '5656', '3434'],
        'ad_id': ['3333_4545_keyword_1', '4444_6666_keyword_2', 'test_ads_facebook', 'blogger_a_channel_b']
       }

key_ = 'medium'
value_ = 'cpc'

struct_example = AdId(structure = structure_dict, key =  key_, value = value_)
struct_example

"""
Results:

Key: medium
Value: cpc
Sources: ['google', 'google', 'youtube']
Mediums: ['cpc', 'cpc', 'cpc']
Campaign Ids: ['111', '222', 'aaa']
Ad Group Ids: ['3333', '4444', '3434']
Ad_ids: ['3333_4545_keyword_1', '4444_6666_keyword_2', 'blogger_a_channel_b']

"""