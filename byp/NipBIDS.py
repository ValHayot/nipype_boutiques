from nipype import Workflow, MapNode, Node, Function
from nipype.interfaces.utility import IdentityInterface, Function
import os, json, time
from Sim import Sim

class NipBIDS(Sim):

    def __init__(self, boutiques_descriptor, bids_dataset, output_dir, options={}):
     
        super().__init__(os.path.abspath(boutiques_descriptor), bids_dataset, output_dir)
        
        # Includes: skip_participant_analysis,
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
            participants.inputs.data_dir = self.input_path
            participants.inputs.skipped = self.skipped_participants



            p_analysis = MapNode(Function(input_names=['nib', 'analysis_level', 
                                      'participant_label','working_dir'],
                                      output_names=['result'],
                                      function=run_analysis),
                                      iterfield=['participant_label'],
                                      name='run_participant_analysis')

            wf.add_nodes([participants])            
            wf.connect(participants, 'out', p_analysis, 'participant_label')
            
            p_analysis.inputs.analysis_level = 'participant'
            p_analysis.inputs.nib = self
            p_analysis.inputs.working_dir = os.getcwd()


        # Group analysis
        if self.do_group_analysis:
            groups = Node(Function(input_names=['nib', 'analysis_level', 'bids_dataset', 
                                'working_dir', 'dummy_token'],
                                output_names=['g_result'],
                                function=run_analysis),
                                name='run_group_analysis')

            groups.inputs.analysis_level = 'group'
            groups.inputs.nib = self
            groups.inputs.working_dir = os.getcwd()

        
            if p_analysis is not None:
                wf.connect(p_analysis, 'result', groups, 'dummy_token')
            else:
                wf.add_nodes([groups])
            
        eg = wf.run()
        
        if self.do_participant_analysis:        
            for res in eg.nodes()[1].result.outputs.result:
                self.pretty_print(res)

        if self.do_group_analysis and self.do_participant_analysis:
            self.pretty_print(eg.nodes()[2].result.outputs.g_result)
        elif self.do_group_analysis:
            self.pretty_print(eg.nodes()[0].result.outputs.g_result)

            
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

def run_analysis(nib, analysis_level, working_dir, participant_label=None, dummy_token=None):
    import os

    out_key = None

    if analysis_level == "group":
        invocation_file = "./invocation-group.json"
        out_key = "group"
    else:
        invocation_file = "./invocation-{0}.json".format(participant_label)
        out_key = "participant_label"

    nib.write_invocation_file(analysis_level, participant_label, invocation_file)
    exec_result = nib.bosh_exec(invocation_file, working_dir)
    os.remove(invocation_file)

    return (out_key, exec_result)



def get_participants(data_dir, skipped):

    from bids.grabbids import BIDSLayout

    layout = BIDSLayout(data_dir)
    participants = layout.get_subjects()    
    
    return list(set(participants) - set(skipped))
