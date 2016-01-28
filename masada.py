#!/usr/bin/env python2.7
import get_pbeast_data
import sys
import os
from peewee import *
import json
from sqlite3 import OperationalError, IntegrityError
import logging
import argparse
import dateutil.parser
import copy
import argparse


root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

DB_FILE = os.path.abspath('pbeast_data_report.db')
db = SqliteDatabase(DB_FILE)

class InitializeAction(object):
    def __init__(self):
        if os.path.isfile(DB_FILE):
            os.remove(DB_FILE)
        self.create_tables()

    def create_tables(self):
        db.connect()

        tables = [klass for klass in BaseModel.__subclasses__()]
        db.create_tables(tables)
        db.close() 
        

class NoneAction(object):
    def __init__(self, **kwargs):
        pass


class BaseModel(Model):
    class Meta:
        database = db


class DatasetRetrieveAction(): 
    def __init__(self, namespace):

        is_single = namespace['single']

        if is_single:
            self.single_dispatch(namespace)
        else:
            self.multiple_dispatch(namespace)

    def multiple_dispatch(self, namespace):
        ds_class_name = namespace['dataset']
        obj = None
        klass = None
        if ds_class_name in datasets:
            klass = getattr(sys.modules[__name__], ds_class_name)

        l_input_params = []
        for lumi in LumiBlockModel.select().where(LumiBlockModel.run_number==namespace['run_number']):
            input_params = copy.copy(namespace)
            input_params['stime'] = lumi.stime
            input_params['etime'] = lumi.etime
            input_params['lumiblock'] = lumi.lumiblock
            l_input_params.append(input_params)


        for input_params in l_input_params: 
            data_set = klass(**input_params)
            data_set.get_data()
            data_set.parse_input()
            data_set.transform()
            data_set.insert()
            logging.info("Data object %s stored" % ds_class_name)

    def single_dispatch(self, namespace):
        ds_class_name = namespace['dataset']
        obj = None
        klass = None
        if ds_class_name in datasets:
            klass = getattr(sys.modules[__name__], ds_class_name)

        input_params = namespace

        data_set = klass(**input_params)
        data_set.get_data()
        data_set.parse_input()
        data_set.transform()
        data_set.insert()
        logging.info("Data object %s stored" % ds_class_name)


class Dataset(object):
    def parse_params(self, *args, **kwargs):
        raise NotImplementedError()

    def get_data(self, *args, **kwargs):
        raise NotImplementedError()

    def parse_input(self, *args, **kwargs):
        raise NotImplementedError()

    def transform(self, *args, **kwargs):
        raise NotImplementedError()
    
    def insert(self):
        raise NotImplementedError()

class RunDS(Dataset):
    info = "Basic dataset with the Run number and the start - end times"
    def __init__(self, *args, **kwargs):
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.model = RunModel(**self.parsed_params)
        self.datasource = None

    def parse_params(self, *args, **kwargs):
        params = {}
        params['stime'] = int(kwargs['stime'])
        params['etime'] = int(kwargs['etime'])
        params['run_number'] = int(kwargs['run_number'])
        return params

    def get_data(self, *args, **kwargs):
        logging.info("Dataset does not need to retrieve, ignoring")

    def parse_input(self, *args, **kwargs):
        logging.info("Dataset does not need to parse, ignoring")

    def transform(self, *args, **kwargs):
        logging.info("Dataset does not need to transform, ignoring")

    def insert(self):
        logging.info("Inserted Run Object")
        self.model.save(force_insert=True)


class AllLumiblocksDS(Dataset):
    info = "Compound dataset to retrieve segmented Run information."
    def __init__(self, *args, **kwargs):
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.model = LumiBlockModel
        self.models = []
        self.datasource = None

    def parse_params(self, *args, **kwargs):
        params = {}
        params['run_number'] = kwargs['run_number']
        params['lumiblocks'] = kwargs['lumiblocks']
        params['length'] = kwargs.get('length', 60) # Defaults to 60 seconds
        return params

    def get_data(self, *args, **kwargs):
        logging.info("Dataset does not need to retrieve, ignoring")

    def parse_input(self, *args, **kwargs):
        logging.info("Dataset does not need to parse, ignoring")

    def transform(self, *args, **kwargs):
        logging.info("Retrieving Run data and generating the evenly spaced time segments")
        try:
            run_number = self.parsed_params['run_number']
            run = RunModel.get(run_number=run_number)
            start = run.stime
            end = run.etime

            start_segments = range(start, end, (end - start) / self.parsed_params['lumiblocks'])
            end_segments = [stime + self.parsed_params['length'] for stime in start_segments]
            time_intervals = zip(start_segments, end_segments)

            for i, (stime, etime) in enumerate(time_intervals): 
                params = {}
                params['stime'] = stime
                params['etime'] = etime
                params['run_number'] = run_number
                params['lumiblock'] = i
                self.models.append(params) 
        except Exception as e:
            raise Exception("Run number is not registered, try running \
                    'retrieve %s --dataset RunDS'" % run_number)

    def insert(self):
        logging.info("Inserted multiple Lumiblocks Object")
        db.connect()
        with db.transaction(): 
            self.model.insert_many(self.models).execute()
        db.close() 

        
class LumiblockDS(Dataset):
    info = "Lumiblock dataset, a subset of a Run."
    def __init__(self, *args, **kwargs):
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.model = LumiBlockModel(**self.parsed_params)
        self.datasource = None

    def parse_params(self, *args, **kwargs):
        params = {}
        params['stime'] = kwargs['stime']
        params['etime'] = kwargs['etime']
        params['run_number'] = int(kwargs['run_number'])
        params['lumiblock'] = int(kwargs['stime'])
        return params

    def get_data(self, *args, **kwargs):
        logging.info("Dataset does not need to retrieve, ignoring")

    def parse_input(self, *args, **kwargs):
        logging.info("Dataset does not need to parse, ignoring")

    def transform(self, *args, **kwargs):
        logging.info("Dataset does not need to transform, ignoring")

    def insert(self):
        logging.info("Inserted Lumiblock Object")
        self.model.save(force_insert=True)

class PBeastDSMethods(object):
    def parse_params(self, *args, **kwargs):
        params = {}
        params['stime'] = kwargs['stime']
        params['etime'] = kwargs['etime']
        params['lumiblock'] = kwargs['lumiblock']
        params.update(self.datasource.get_properties())
        return params

    def get_data(self, *args, **kwargs):
        logging.info("Retrieving data points from PBeast")
        try:
            self.pbeast_data = json.loads(get_pbeast_data.get(**self.parsed_params))
        except Exception as e:
            import pdb; pdb.set_trace()


    def parse_input(self, *args, **kwargs):
        self.parsed_pbeast = self.datasource.parse_input(self.pbeast_data)

    def transform(self, *args, **kwargs):
        logging.info("Transforming data points from PBeast")
        import pdb; pdb.set_trace()
        self.models.extend(self.datasource.transform(self.parsed_pbeast,
            self.parsed_params['lumiblock']))

    def insert(self):
        logging.info("Inserting RoSInput objects to DB")
        db.connect()
        with db.transaction(): 
            self.model.insert_many(self.models).execute()
        db.close() 


class TrafficShappingCreditsDS(PBeastDSMethods, Dataset):
    info = "L1 Event latency"
    def __init__(self, *args, **kwargs):
        self.datasource = CreditsDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = CreditsModel
        self.models = []
        self.pbeast_data = []

class L1EventLatencyDS(PBeastDSMethods, Dataset):
    info = "L1 Event latency"
    def __init__(self, *args, **kwargs):
        self.datasource = L1LatencyDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class RejectedEventLatencyDS(PBeastDSMethods, Dataset):
    info = "Reject Event latency"
    def __init__(self, *args, **kwargs):
        self.datasource = RejectedEventLatencyDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class RejectedEventAmountDS(PBeastDSMethods, Dataset):
    info = "Accepted events per time unit per rack"
    def __init__(self, *args, **kwargs):
        self.datasource = RejectedEventsDatasource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class AcceptedEventAmountDS(PBeastDSMethods, Dataset):
    info = "Accepted events per time unit per rack"
    def __init__(self, *args, **kwargs):
        self.datasource = AcceptedEventsDatasource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class ActiveTPUsPerRackAmountDS(PBeastDSMethods, Dataset):
    info = "Accepted Event latency"
    def __init__(self, *args, **kwargs):
        self.datasource = ActiveTPUsPerRackDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class AcceptedEventLatencyDS(PBeastDSMethods, Dataset):
    info = "Accepted Event latency"
    def __init__(self, *args, **kwargs):
        self.datasource = AcceptEventLatencyDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = EventLatencyModel
        self.models = []
        self.pbeast_data = []


class HLTInputRateDS(PBeastDSMethods, Dataset):
    info = "HLTSV Input rate measured in DFSummary"
    def __init__(self, *args, **kwargs):
        self.datasource = HLTInputRateDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = HLTInputRateModel
        self.models = []
        self.pbeast_data = []


class RoSInputBandwidthDS(PBeastDSMethods, Dataset): 
    info = " Input bandwidth for the RoS, useful to analyze event sizes."
    def __init__(self, *args, **kwargs):
        self.datasource = ROSBWDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = RoSModel
        self.models = []
        self.pbeast_data = []

class RoSEnabledDS(PBeastDSMethods, Dataset): 
    info = "Boolean value to filter disabled RoSes from useful datasets like input bandwidth or input rate."
    def __init__(self, *args, **kwargs):
        self.datasource = ROSEnabledDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = RoSModel
        self.models = []
        self.pbeast_data = []
        
class RoSInputRateDS(PBeastDSMethods, Dataset): 
    info = "RoS Input rate measured in Hz (fragments per seconds)."
    def __init__(self, *args, **kwargs):
        self.datasource = ROSInputRateDataSource()
        self.parsed_params = self.parse_params(*args, **kwargs)
        self.lumiblock = LumiblockDS(*args, **kwargs)
        self.model = RoSModel
        self.models = []
        self.pbeast_data = []


class RunModel(BaseModel):
    run_number = DecimalField(primary_key=True)
    stime = DecimalField()
    etime = DecimalField()

class LumiBlockModel(BaseModel):
    run_number = ForeignKeyField(RunModel)
    lumiblock = DecimalField(null=False)
    stime = DecimalField()
    etime = DecimalField()

    class Meta:
        indexes = (
                # Specify a unique multi-column index on from/to-user.
                (('run_number', 'lumiblock'), True),
                )

class HLTInputRateModel(BaseModel):
    lumiblock = ForeignKeyField(LumiBlockModel)
    t = DecimalField()
    var_name = CharField()
    req = FloatField()

    class Meta:
        primary_key = CompositeKey('t', 'lumiblock', 'var_name')
        database = db 

class CreditsModel(BaseModel):
    lumiblock = ForeignKeyField(LumiBlockModel)
    t = DecimalField()
    entity_name = CharField()
    var_name = CharField()
    req = FloatField()

    class Meta:
        primary_key = CompositeKey('t', 'lumiblock','entity_name', 'var_name')
        database = db 


class EventLatencyModel(BaseModel):
    lumiblock = ForeignKeyField(LumiBlockModel)
    t = DecimalField()
    entity_name = CharField()
    var_name = CharField()
    req = FloatField()

    class Meta:
        primary_key = CompositeKey('t', 'lumiblock','entity_name', 'var_name')
        database = db 

class RoSModel(BaseModel):
    lumiblock = ForeignKeyField(LumiBlockModel)
    t = DecimalField()
    ros_name = CharField()
    channel = DecimalField()
    var_name = CharField()
    min_req = FloatField()
    req = FloatField()
    max_req = FloatField() 

    class Meta:
        primary_key = CompositeKey('t', 'ros_name', 'channel', 'var_name')
        database = db 

datasets = [kls.__name__ for kls in Dataset.__subclasses__()]
datasets_info = [kls.info for kls in Dataset.__subclasses__()]

class DataSources(object):
    pass

class MultipleMeasureDataSources(DataSources): 
    def transform(self, objects_list, lumiblock):
        transformed_obj_list = []
        for var_name, t, value in objects_list:
            s_var_name = var_name.split('.')[2]
            entity_name = var_name.split('.')[4]

            ds = {"lumiblock": lumiblock,
                    "t": t,
                    "entity_name": entity_name,
                    "var_name": s_var_name,
                    "req": value}

            transformed_obj_list.append(ds)
        return transformed_obj_list


    def parse_input(self, json_obj): 
        rows = []
        for data in json_obj:
            data_name = data["label"]
            for d in data["datapoints"]:
                t = d[0]
                value = d[1]
                row = [data_name, t, value]
                rows.append(row)
        return rows

class SingleMeasureDataSources(DataSources): 
    def transform(self, objects_list, lumiblock):
        transformed_obj_list = []
        for var_name, t, value in objects_list:
            var_name = var_name.split('.')[2]

            ds = {"lumiblock": lumiblock,
                    "t": t,
                    "var_name": var_name,
                    "req": value}

            transformed_obj_list.append(ds)
        return transformed_obj_list


    def parse_input(self, json_obj): 
        rows = []
        for data in json_obj:
            data_name = data["label"]
            for d in data["datapoints"]:
                t = d[0]
                value = d[1]
                row = [data_name, t, value]
                rows.append(row)
        return rows
    

class HLTInputRateDataSource(SingleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "DFSummary",
                "server": "DF",
                "attrib": "DCML1Rate",
                "object_regxp": "DFSummary"
                }

        
class CreditsDataSource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "DCM",
                "server": "DF_IS",
                "attrib": "DcTrafficShapingCredits",
                "object_regxp": ".*DCM:HLT-[0-9]+:tpu-rack-[0-9]+:pc-tdq-tpu-[0-9]+.info"
                } 

class L1LatencyDataSource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "HLTMPPUInfo",
                "server": "DF_IS",
                "attrib": "AverageL1ResultTime",
                "object_regxp": ".*MTS:tpu-rack-[0-9]+.PU_ChildInfo"
                } 


class RejectedEventLatencyDataSource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "HLTMPPUInfo",
                "server": "DF_IS",
                "attrib": "AverageRejectTime",
                "object_regxp": ".*MTS:tpu-rack-[0-9]+.PU_ChildInfo"
                } 


class ActiveTPUsPerRackDataSource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",                                                 
                "typ3": "HLTMPPUMotherInfo",                                          
                "server": "DF_IS",                                                    
                "attrib": "NumActive",                                                
                "object_regxp": ".*DefMIG-IS:HLT-[0-9]+:tpu-rack-[0-9]+.PU_MotherInfo"
                } 
        


class AcceptEventLatencyDataSource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "HLTMPPUInfo",
                "server": "DF_IS",
                "attrib": "AverageAcceptTime",
                "object_regxp": ".*DefMIG-IS:HLT-[0-9]+:tpu-rack-[0-9]+.PU_ChildInfo"
                } 


class RejectedEventsDatasource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "DCM",
                "server": "DF_IS",
                "attrib": "ProxRejEvents",
                "object_regxp": ".*DefMIG-IS:HLT-[0-9]+:tpu-rack-[0-9]+.*"
                } 

class AcceptedEventsDatasource(MultipleMeasureDataSources):
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "DCM",
                "server": "DF_IS",
                "attrib": "ProxAccEvents",
                "object_regxp": ".*DefMIG-IS:HLT-[0-9]+:tpu-rack-[0-9]+.*"
                } 



class ROSDataSource(DataSources):
    def __init__(self): 
        self.header = ["ros.name", "t", "channel", "min.req", "req",
                              "max.req"] 

    def transform(self, objects_list, lumiblock):
        transformed_obj_list = []
        for ros_name, t, channel, min_req, req, max_req in objects_list:
            ros_vars_name = ros_name.split('.')
            n_ros_name = ".".join(ros_vars_name[-2:])
            var_name = ros_vars_name[2]

            ds = {"lumiblock": lumiblock, "t": t,
                    "ros_name": n_ros_name,
                    "channel": int(channel),
                    "var_name": var_name, "min_req": float(min_req),
                    "req": float(req), "max_req": float(max_req)}
            transformed_obj_list.append(ds)
        return transformed_obj_list


    def parse_input(self, json_obj): 
        rows = []
        for ros in json_obj:
            ros_name = ros["label"]
            for d in ros["datapoints"]:
                ep = d[0]
                chans = d[1:]
                if chans[0]:
                    try:
                        for i, c in enumerate(chans[0]):
                            if c:
                                if len(c) == 1:
                                    c = c * 3
                                r = [ros_name, ep, i]
                                r.extend(c)
                                rows.append(r)
                    except Exception as e:
                        print(e)
        return rows


class ROSBWDataSource(ROSDataSource): 
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "RobinNPDescriptorReadoutModuleInfo",
                "server": "DF",
                "attrib": "rolInputBandwidth",
                "object_regxp": "ROS.*.ReadoutModule[0-1]"
                }

class ROSEnabledDataSource(ROSDataSource): 
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "RobinNPDescriptorReadoutModuleInfo",
                "server": "DF",
                "attrib": "rolEnabled",
                "object_regxp": "ROS.*.ReadoutModule[0-1]"
                } 
        
class ROSInputRateDataSource(ROSDataSource): 
    def get_properties(self):
        return {
                "partition": "ATLAS",
                "typ3": "RobinNPDescriptorReadoutModuleInfo",
                "server": "DF",
                "attrib": "level1RateHz",
                "object_regxp": "ROS.*.ReadoutModule[0-1]"
                }


    
def main():
    """Main entry for the load mapping script"""
    run_lumi_block = None
    try:
        # create_tables()
        # complete the Run number and lumiblock data with defaults:
        run_start_date = dateutil.parser.parse("Tue Oct 06, 19:21").strftime("%s")
        run_end_date = dateutil.parser.parse("Wed Oct 07, 19:48").strftime("%s")
        run_item = RunModel(run_number=281143,
                                stime=run_start_date,
                                etime=run_end_date)
        run_item.save(force_insert=True)

        lumiblock_st = dateutil.parser.parse("Wed, 07 Oct 2015 12:30"). \
                            strftime("%s")
        lumiblock_et = dateutil.parser.parse("Wed, 07 Oct 2015 12:31"). \
                            strftime("%s") 

        run_lumi_block = LumiBlockModel(run_number=run_item.run_number,
                                            lumiblock=1,
                                            stime=lumiblock_st,
                                            etime=lumiblock_et)
        run_lumi_block.save(force_insert=True)
        # populate_tables(ROSEnabledDataSource, ROSDataSet, lumi=run_lumi_block)

    except Exception as op:
        logging.debug("Database already exists, ignoring")
        print("Database already exists, ignoring", op)

    # db.close()


def populate_tables(data_source, data_sink, **kwargs):
    """Create the DB and load it with the default pbeast data"""
    try:
        with db.transaction(): 
            data = data_source()

            params = copy.copy(kwargs)
            params['stime'] = params['lumi'].stime
            params['etime'] = params['lumi'].etime
            del params['lumi']

            params.update(data.get_properties())
            resp = get_pbeast_data.get(**params)

            resp_json = json.loads(resp)
            resp_array = data.parse_input(resp_json)

            try:
                insert_rows = data.transform(resp_array)
                for ir in insert_rows:
                    ir['run_lumi'] = kwargs['lumi']
                    data_sink.insert(ir).execute()
            except IntegrityError:
                logging.debug("Duplicated row, skippin'")

    except Exception as e:
        print(e)

if __name__ == '__main__':
    help_text = ["%s - %s" % (ds, dinfo) for (ds, dinfo) in zip(datasets, datasets_info)]

    parser = argparse.ArgumentParser(description='PBeast to Simulation parameters and metrics ETL module')

    subparsers = parser.add_subparsers(dest='subparser', title='Available commands', \
            description="Commands to interact with the retrieval, statistics and initalization routines")

    init_parser = subparsers.add_parser('initialize', help="Creates the SQLite data tables")
    init_parser.add_argument('--all', dest='initialize', action='store_const', const=InitializeAction, default=NoneAction)
    
    retrieve_parser = subparsers.add_parser('retrieve',
            help="Retrieve configured datasets from PBeast database",
            formatter_class=argparse.RawDescriptionHelpFormatter)

    retrieve_parser.add_argument('--stime', dest='stime', type=int, help='select the start date for the measurement')
    retrieve_parser.add_argument('--etime', dest='etime', type=int, help='select the end date for the measurement')
    retrieve_parser.add_argument('run_number', type=int, help='select the run number for the dataset')
    retrieve_parser.add_argument('--lumiblock', type=int, help="select the lumiblock for the dataset")
    retrieve_parser.add_argument('--lumiblocks', type=int, help="For AllLumiblocks DS select the amount of lumiblocks to partition the run into")
    retrieve_parser.add_argument('--length', type=int, help="For AllLumiblocks DS select the size (seconds) of each lumiblock") 
    retrieve_parser.add_argument('--single', dest='single', action='store_true', help="Retrieve single dataset. If chosen then select lumiblock / stime / etime")
    retrieve_parser.add_argument('--multiple', dest='single', action='store_false', help="Retrieve multiple datasets with time data from the lumiblocks datasets")
    retrieve_parser.set_defaults(single=True)

    
    retrieve_parser.add_argument('--dataset', type=str, dest='dataset', default=None, \
            help="""Retrieve the datasets for a run number, a lumiblock and a time interval.
If neither exists they are created at runtime.
\tAvailable datasets are:
%s""" % (" ### ".join(help_text)))

    # The parse_args executes the DatasetRetrieve action
    args = parser.parse_args()
    if args.subparser == 'initialize':
        args.initialize()
    if args.subparser == 'retrieve':
        DatasetRetrieveAction(vars(args))
    # main()
