from nipype import Workflow, MapNode, Node, Function
from nipype.interfaces.utility import IdentityInterface, Function
import os, json, time

class NipBIDS(object):

    def __init__(self, boutiques_descriptor, bids_dataset, output_dir, options={}):
     
        self.boutiques_descriptor = os.path.join(os.path.abspath(boutiques_descriptor))
        self.bids_dataset = bids_dataset
        self.output_dir = output_dir
        
        # Includes: use_hdfs, skip_participant_analysis,
        # skip_group_analysis, skip_participants_file
        for option in list(options.keys()): setattr(self, option, options.get(option))

        # Check what will have to be done
        self.do_participant_analysis = self.supports_analysis_level("participant") \
                                                            and not self.skip_participant_analysis
        self.do_group_analysis = self.supports_analysis_level("group") \
                                                 and not self.skip_group_analysis
        self.skipped_participants = self.skip_participants_file.read().split() if self.skip_participants_file else []

        # Print analysis summary
        print("Computed Analyses: Participant [ {0} ] - Group [ {1} ]".format(str(self.do_participant_analysis).upper(),
                                                                              str(self.do_group_analysis).upper()))
     
        if len(self.skipped_participants):
            print("Skipped participants: {0}".format(self.skipped_participants)) 

    def run(self):

        wf = Workflow('bapp')
        wf.base_dir = os.getcwd()

        # group analysis can be executed if participant analysis is skipped
        p_analysis = None

        # Participant analysis
        if self.do_participant_analysis:

            participants = Node(Function(input_names=['data_dir', 'skipped'],
                                            output_names=['out'],
                                            function=get_participants),
                                    name='get_participants')
            participants.inputs.data_dir = self.bids_dataset
            participants.inputs.skipped = self.skipped_participants



            p_analysis = MapNode(Function(input_names=['analysis_level', 'participant_label', 
                                        'boutiques_descriptor', 'bids_dataset', 'output_dir', 'working_dir'],
                                      output_names=['result'],
                                      function=run_analysis),
                                      iterfield=['participant_label'],
                                      name='run_participant_analysis')

            wf.add_nodes([participants])            
            wf.connect(participants, 'out', p_analysis, 'participant_label')
            
            p_analysis.inputs.analysis_level = 'participant'
            p_analysis.inputs.boutiques_descriptor = self.boutiques_descriptor
            p_analysis.inputs.bids_dataset = self.bids_dataset
            p_analysis.inputs.output_dir = self.output_dir
            p_analysis.inputs.working_dir = os.getcwd()


            #print(get_participants(self.bids_dataset, self.skipped_participants))

            #for result in mapped.collect():
            #    self.pretty_print(result)

        # Group analysis
        if self.do_group_analysis:
            groups = Node(Function(input_names=['analysis_level', 'bids_dataset', 
                                'boutiques_descriptor', 'output_dir', 'working_dir', 'dummy_token'],
                                output_names=['g_result'],
                                function=run_analysis),
                                name='run_group_analysis')

            groups.inputs.analysis_level = 'group'
            groups.inputs.bids_dataset = self.bids_dataset
            groups.inputs.boutiques_descriptor = self.boutiques_descriptor
            groups.inputs.output_dir = self.output_dir
            groups.inputs.working_dir = os.getcwd()

        
            if p_analysis is not None:
                wf.connect(p_analysis, 'result', groups, 'dummy_token')
            else:
                wf.add_nodes([groups])
                #group_result = run_group_analysis()
                #self.pretty_print(group_result)

            
        eg = wf.run()
        
        for res in eg.nodes()[1].result.outputs.result:
            self.pretty_print(res)
        self.pretty_print(eg.nodes()[2].result.outputs.g_result)
            
    def supports_analysis_level(self,level):
        desc = json.load(open(self.boutiques_descriptor))
        analysis_level_input = None
        for input in desc["inputs"]:
            if input["id"] == "analysis_level":
                analysis_level_input = input
                break
        assert(analysis_level_input),"BIDS app descriptor has no input with id 'analysis_level'"
        assert(analysis_level_input.get("value-choices")),"Input 'analysis_level' of BIDS app descriptor has no 'value-choices' property"   
        return level in analysis_level_input["value-choices"]

        
        

    def create_tar_file(self, out_dir, tar_name, files):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        with tarfile.open(os.path.join(out_dir, tar_name), "w") as tar:
            for f in files:
                tar.add(f)

    def pretty_print(self, result):
        
        (label, (log, returncode)) = result
        status = "SUCCESS" if returncode == 0 else "ERROR"
        timestamp = str(int(time.time() * 1000))
        filename = "{0}.{1}.log".format(timestamp, label)
        with open(filename,"w") as f:
            f.write(str(log, "utf-8"))
        print(" [ {3} ({0}) ] {1} - {2}".format(returncode, label, filename, status))


    def get_bids_dataset(self, data, participant_label):

        filename = 'sub-{0}.tar'.format(participant_label)
        tmp_dataset = 'temp_dataset'    
        foldername = os.path.join(tmp_dataset, 'sub-{0}'.format(participant_label))

        # Save participant byte stream to disk
        with open(filename, 'w') as f:
            f.write(data)

        # Now extract data from tar
        tar = tarfile.open(filename)
        tar.extractall(path=foldername)
        tar.close()

        os.remove(filename)

        return os.path.join(tmp_dataset, os.path.abspath(self.bids_dataset))


    def is_valid_file(parser, arg):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return open(arg, 'r')


def run_analysis(analysis_level, bids_dataset, boutiques_descriptor, 
            working_dir, output_dir, participant_label=None, dummy_token=None):
    import os

    def write_invocation_file(invocation_file):

        import json, errno
        # Note: the invocation file format will change soon

        # Creates invocation object
        invocation = {}
        invocation["bids_dir"] = bids_dataset
        invocation["output_dir_name"] = output_dir
        if analysis_level == "participant":
            invocation["analysis_level"] = "participant"
            invocation["participant_label"] = participant_label
        elif analysis_level == "group":
            invocation["analysis_level"] = "group"

        json_invocation = json.dumps(invocation)

        # Writes invocation
        with open(invocation_file,"w") as f:
            print(invocation_file)
            f.write(json_invocation)
        try:
            os.mkdir(output_dir)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(output_dir):
                pass
            else:
                raise

    def bosh_exec(invocation_file):
        import subprocess
        run_command = "bosh {0} -i {1} -e -v {2}:{2}".format(boutiques_descriptor, invocation_file, working_dir)
        result = None
        try:
            log = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
            result = (log, 0)
        except subprocess.CalledProcessError as e:
            result = (e.output, e.returncode)
        return result


    if analysis_level == "group":
        invocation_file = "{0}/invocation-group.json".format(working_dir)
        write_invocation_file(invocation_file)
        exec_result = bosh_exec(invocation_file)
        os.remove(invocation_file)
        return ("group", exec_result)
    else:
        invocation_file = "{0}/invocation-{1}.json".format(working_dir, participant_label)
        write_invocation_file(invocation_file)

        exec_result = bosh_exec(invocation_file)
        os.remove(invocation_file)
        return (participant_label, exec_result)



def get_participants(data_dir, skipped):

    from bids.grabbids import BIDSLayout

    layout = BIDSLayout(data_dir)
    participants = layout.get_subjects()    
    
    return list(set(participants) - set(skipped))
