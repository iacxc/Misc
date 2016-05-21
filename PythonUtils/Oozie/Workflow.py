"""
   class to represent a workflow.xml
"""

import sys
import xml.etree.ElementTree as ElementTree

from Node import *


class Workflow(Node):
    type = 'workflow-app'
    def __init__(self, filename="workflow.xml"):
        self.name = None
        self.ns= None
        self.nodes = []
        self.read(filename)
        
    def read(self, filename):
        xml = ElementTree.parse(filename)
        xmlroot = xml.getroot()
        
        match = re.match(r'{(.*)}', xmlroot.tag)

        if match:
            self.ns = match.group(1)
        
        self.name = xmlroot.get('name')

        for elem in xmlroot:
            elem_type = self.remove_ns(elem.tag)
            klass = {'start' : Start,
                     'end'   : End,
                     'kill'  : Kill,
                     'fork'  : Fork,
                     'join'  : Join,
                     'decision'  : Decision,
                     'action'  : Action,
                     }.get(elem_type)
            node = klass(elem)
            self.nodes.append(node)
            
    @property
    def opentag(self):
        return '<{type} xmlns="{ns}" name="{name}">'.format(
            type=self.type, ns=self.ns, name=self.name)
    
    @property
    def closetag(self):
        return '</{type}>'.format(type=self.type)
        
    def dump(self, fs=sys.stdout):
        fs.write('Workflow: {0}\n'.format(self.name))
        for node in self.nodes:
            node.dump(fs, '   ')
        
        
if __name__ == '__main__':
    fname = sys.argv[1] if len(sys.argv) > 1 else "workflow.xml"
    
    wf = Workflow(fname)
    wf.dump()
    
        