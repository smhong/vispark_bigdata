
class VisparkMeta(object):
    def __init__(self):
        self.name = ''

        self.split_shape     = {}
        self.split_position  = {}

        self.data_split      = {}
        self.data_shape      = {}
        self.buffer_shape    = {}
        self.full_data_shape = {}

        self.data_halo = 0
        self.halo_updated = True
        
        self.data_type = None   
        self.data_kind = None

        self.comm_type = None   
        self.process_type = None

        self.data = None


    def getDict(self):
        metaDict={}

        metaDict['name'] = self.name

        metaDict['split_shape'] = self.split_shape
        metaDict['split_position'] = self.split_position

        metaDict['data_split'] = self.data_split
        metaDict['data_shape'] = self.data_shape
        metaDict['buffer_shape'] = self.buffer_shape
        metaDict['full_data_shape'] = self.full_data_shape

        metaDict['data_halo'] = self.data_halo
        metaDict['halo_updated'] = self.halo_updated
        
        metaDict['data_type'] = self.data_type
        metaDict['data_kind'] = self.data_kind
        
        metaDict['comm_type'] = self.comm_type

        metaDict['data'] =self.data

        return metaDict

    def _print(self):
        print "VISPARK META"
        print "========================"
        print "name",self.name
        print "data_shape", self.data_shape
        print "full_data_shape", self.full_data_shape
        print "buffer_shape", self.buffer_shape
        print "\ndata_type", self.data_type
        print "data_kind", self.data_kind


