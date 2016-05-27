

import re
import sys
from collections import OrderedDict

def get_namespace(tag):
    match = re.match(r'{(.*)}', tag)

    return match.group(1) if match else None


def remove_namespace(tag):
    return re.sub(r'{.*}', '', tag)


class Node(object):
    def __init__(self, namespace=None, text=''):
        self.ns = namespace
        self.text = text
        self.attrs = OrderedDict()
        self.nodes = []


    @classmethod
    def parse(cls, rootelem):
        raise NotImplementedError('Node')


    def addattr(self, name, value):
        self.attrs[name] = value
        return self


    def get(self, name):
        return self.attrs[name]


    def append(self, node):
        self.nodes.append(node)
        return self


    def extend(self, nodelist):
        self.nodes.extend(nodelist)
        return self


    @property
    def type(self):
        t = self.__class__.__name__.lower()
        if t == 'workflow':
            t = 'workflow-app'

        return t


    @property
    def xmlcontent(self):
        if len(self.nodes) > 0:
            return ''.join(n.dumpxml() for n in self.nodes)
        elif self.text:
            return self.text

        return ''


    @property
    def opentag(self):
        xmlstr = '<{0}'.format(self.type)

        if self.ns:
            xmlstr += ' xmlns="{0}"'.format(self.ns)


        for name, value in self.attrs.items():
            xmlstr += ' {0}="{1}"'.format(name, value)

        if self.xmlcontent == '':
            xmlstr += '/>'
        else:
            xmlstr += '>'

        return xmlstr


    @property
    def closetag(self):
        return '</{type}>'.format(type=self.type)


    def dumpxml(self):
        if self.xmlcontent == '':
            return self.opentag
        else:
            return '{0}{1}{2}'.format(self.opentag, self.xmlcontent, self.closetag)


    def dump(self, fs, indent):
        fs.write('{0}{1}:'.format(indent, self.type))
        if self.ns:
            fs.write(' xmlns->"{0}"'.format(self.ns))
        if self.text:
            fs.write(' "{0}"'.format(self.text))
        fs.write(' ' + ','.join('{0}->"{1}"'.format(name, value)
                           for name, value in self.attrs.items()))

        fs.write('\n')
        for node in self.nodes:
            node.dump(fs, indent + '   ')


    @property
    def subname(self):
        return 'nodes'


    def json(self):
        jsonobj = OrderedDict()
        for name, value in self.attrs.items():
            jsonobj[name] = value

        if self.ns:
             jsonobj['namespace'] = self.ns

        if self.text:
            if len(jsonobj) == 0:
                return {self.type: self.text}
            else:
                jsonobj['text'] = self.text

        if len(self.nodes) > 0:
            jsonobj[self.subname] = [node.json() for node in self.nodes]

        return {self.type: jsonobj}


def SimpleNode(cls):
    def __init__(self, attrs={}, text=''):
        Node.__init__(self)
        self.text = text.strip()
        for name, value in attrs.items():
            self.addattr(name, value)

    return type(cls, (Node,), {'__init__' : __init__})


#simple classes
Path = SimpleNode('path')
Case = SimpleNode('case')
Default = SimpleNode('default')
Message = SimpleNode('message')
JobTracker = SimpleNode('job-tracker')
NameNode = SimpleNode('name-node')
Command = SimpleNode('command')
Arg = SimpleNode('arg')
Name = SimpleNode('name')
Value = SimpleNode('value')
Exec = SimpleNode('exec')
Env = SimpleNode('env-var')
File = SimpleNode('file')
Argument = SimpleNode('argument')
CaptureOutput = SimpleNode('capture-output')
Delete = SimpleNode('delete')
Ok = SimpleNode('Ok')
Error = SimpleNode('Error')


class Start(Node):
    def __init__(self, to):
        super(Start, self).__init__()
        self.addattr('to', to)


    @classmethod
    def parse(cls, rootelem):
        return cls(rootelem.get('to'))


class End(Node):
    def __init__(self, name):
        super(End, self).__init__()
        self.addattr('name', name)

    @classmethod
    def parse(cls, rootelem):
        return cls(rootelem.get('name'))


class Fork(Node):
    def __init__(self, name):
        super(Fork, self).__init__()
        self.addattr('name', name)


    @classmethod
    def parse(cls, rootelem):
        fork = cls(rootelem.get('name'))
        for elem in rootelem:
            fork.append(Path({'start' : elem.get('start')}))

        return fork


    @property
    def subname(self):
        return 'pathes'


class Kill(Node):
    def __init__(self, name, message):
        super(Kill, self).__init__()
        self.addattr('name', name)
        if message:
            self.append(message)


    @classmethod
    def parse(cls, rootelem):
        kill = cls(rootelem.get('name'))

        kill.append(Message(text=''.join(rootelem.itertext())))
        return kill


    def json(self):
        jsonobj = OrderedDict({'name': self.get('name')})
        jsonobj.update(self.nodes[0].json())

        return {self.type: jsonobj}


class Decision(Node):
    def __init__(self, name):
        super(Decision, self).__init__()
        self.addattr('name', name)


    @classmethod
    def parse(cls, rootelem):
        deci = cls(rootelem.get('name'))

        for elem in rootelem.getchildren()[0]: # skip the switch
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'case':
                deci.append(Case({'to' : elem.get('to')}, elem.text))
            else:
                deci.append(Default({'to' : elem.get('to')}))

        return deci


    @property
    def open(self):
        return '<{0} name="{1}"><switch>'.format(self.type, self.attrs['name'])


    @property
    def close(self):
        return '</switch></{0}>'.format(self.type)


    @property
    def subname(self):
        return 'predicts'


class Join(Node):
    def __init__(self, name, to):
        super(Join, self).__init__()
        self.addattr('name', name)
        self.addattr('to', to)


    @classmethod
    def parse(cls, rootelem):
        return cls(rootelem.get('name'), rootelem.get('to'))


class Property(Node):
    def __init__(self):
        super(Property, self).__init__()

    @classmethod
    def parse(cls, rootelem):
        prop = Property()
        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'name':
                prop.append(Name(text=elem.text))
            elif elem_type == 'value':
                prop.append(Value(text=elem.text))

        return prop


    def json(self):
        jsonobj = OrderedDict()
        for node in self.nodes:
            jsonobj.update(node.json())

        return {self.type: jsonobj}


class Configuration(Node):
    def __init__(self, props=[]):
        super(Configuration, self).__init__()
        self.extend(props)


    @classmethod
    def parse(cls, rootelem):
        config = cls()
        for elem in rootelem:
            config.append(Property.parse(elem))

        return config


    @property
    def subname(self):
        return 'properties'


class Shell(Node):
    def __init__(self, namespace):
        super(Shell, self).__init__(namespace)


    @classmethod
    def parse(cls, rootelem):
        shell = cls(get_namespace(rootelem.tag))

        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'job-tracker':
                shell.append(JobTracker(text=elem.text))
            elif elem_type == 'name-node':
                shell.append(NameNode(text=elem.text))
            elif elem_type == 'exec':
                shell.append(Exec(text=elem.text))
            elif elem_type == 'env-var':
                shell.append(Env(text=elem.text))
            elif elem_type == 'file':
                shell.append(File(text=elem.text))
            elif elem_type == 'argument':
                shell.append(Argument(text=elem.text))
            elif elem_type == 'configuration':
                shell.append(Configuration.parse(elem))
            elif elem_type == 'capture-output':
                shell.append(CaptureOutput())

        return shell


    @property
    def subname(self):
        return 'parameters'


class Prepare(Node):
    def __init__(self):
        super(Prepare, self).__init__()

    @classmethod
    def parse(cls, rootelem):

        prepare = cls()
        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'delete':
                prepare.append(Delete({'path' : elem.get('path')}))

        return prepare


class Sqoop(Node):
    def __init__(self, namespace):
        super(Sqoop, self).__init__(namespace)


    @classmethod
    def parse(cls, rootelem):
        sqoop = cls(get_namespace(rootelem.tag))

        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'job-tracker':
                sqoop.append(JobTracker(text=elem.text))
            elif elem_type == 'name-node':
                sqoop.append(NameNode(text=elem.text))
            elif elem_type == 'command':
                sqoop.append(Command(text=elem.text))
            elif elem_type == 'arg':
                sqoop.append(Arg(text=elem.text))
            elif elem_type == 'prepare':
                sqoop.append(Prepare.parse(elem))
            elif elem_type == 'configuration':
                sqoop.append(Configuration.parse(elem))

        return sqoop


    @property
    def subname(self):
        return 'parameters'


class Action(Node):
    def __init__(self, name, nodes=[]):
        super(Action, self).__init__()
        self.addattr('name', name)
        self.extend(nodes)


    @classmethod
    def parse(cls, rootelem):
        name = rootelem.get('name')

        action = cls(name)

        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            if elem_type == 'ok':
                action.append(Ok({'to' : elem.get('to')}))
            elif elem_type == 'error':
                action.append(Error({'to' : elem.get('to')}))
            else:
                klass = {'shell' : Shell,
                         'sqoop' : Sqoop}.get(elem_type)
                if klass:
                    action.append(klass.parse(elem))

        return action


    def json(self):
        jsonobj = OrderedDict({'name': self.get('name')})
        for node in self.nodes:
            jsonobj.update(node.json())

        return {self.type: jsonobj}


class MapReduce(Action):
    pass
    
class MapReduce(Action):
    pass
    
class Fs(Action):
    pass
    
class Ssh(Action):
    pass
    
class Pig(Action):
    pass
    
class Java(Action):
    pass
    

class Email(Action):
    pass
    
class Hive(Action):
    pass
    
class Hive2(Action):
    pass
    

class Workflow(Node):
    def __init__(self, namespace, name, nodes=[]):
        super(Workflow, self).__init__(namespace)
        self.addattr('name', name)
        self.extend(nodes)


    @classmethod
    def parse(cls, rootelem):
        ns = get_namespace(rootelem.tag)
        name = rootelem.get('name')

        wf = cls(ns, name)

        for elem in rootelem:
            elem_type = remove_namespace(elem.tag)
            klass = {'start' : Start,
                     'end'   : End,
                     'kill'  : Kill,
                     'fork'  : Fork,
                     'join'  : Join,
                     'decision'  : Decision,
                     'action'  : Action,
                     }.get(elem_type)
            if klass:
                wf.append(klass.parse(elem))

        return wf


def read_workflow(filename):
    import xml.etree.ElementTree as ElementTree

    xml = ElementTree.parse(filename)
    xmlroot = xml.getroot()

    return Workflow.parse(xmlroot)


if __name__ == '__main__':
    import json
    fname = sys.argv[1] if len(sys.argv) > 1 else "workflow.xml"
    
#    wf = read_workflow(fname)

#    print wf.dumpxml()
#    wf.dump(sys.stdout, '')
#    print json.dumps(wf.json())

    wf2 = Workflow('hello-namespace', 'hello', [
            Start('step1'),
            Action('step1', [
                Sqoop('sqoop-namespace')
                    .append(JobTracker(text="a.b.c:8020"))
                    .append(NameNode(text="a.b.c:8050"))
                    .append(Configuration([
                        Property().append(Name(text='mapred.job.queue.name'))
                                  .append(Value(text='default')),
                        Property().append(Name(text='name'))
                                  .append(Value(text='value'))]))
                    .append(Arg(text='import'))
                    .append(Arg(text='-m'))
                    .append(Arg(text='4'))
                ,
                Ok({'to': 'end'}),
                Error({'to' : 'fail'})]),
            Kill('fail', Message(text='Bad operation')),
            End('end')])

    wf2.dump(sys.stdout, '  ')
