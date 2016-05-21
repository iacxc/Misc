

import re


class Node(object):
    def __init__(self, root):
        match = re.match(r'{(.*)}', root.tag)
        if match:
            self.ns = match.group(1)
        else:
            self.ns = None

    @staticmethod
    def remove_ns(tag):
        return re.sub(r'{.*}', '', tag)


    def findelem(self, root, tag):
        return root.find('{{{0}}}{1}'.format(self.ns, tag))


    def dump(self, fs, indent):
        pass
        
        
class Start(Node):
    def __init__(self, root):
        super(Start, self).__init__(root)
        self.to = root.get('to')

    def dump(self, fs, indent):
        fs.write('{0}Start: to -> {1}\n'.format(indent, self.to))


class End(Node):
    def __init__(self, root):
        super(End, self).__init__(root)
        self.name= root.get('name')

    def dump(self, fs, indent):
        fs.write('{0}End: name -> {1}\n'.format(indent, self.name))


class Fork(Node):
    def __init__(self, root):
        class Path(Node):
            def __init__(self, root):
                super(Path, self).__init__(root)
                self.start = root.get('start')
            
            def dump(self, fs, indent):
                fs.write('{0}Path: start -> {1}\n'.format(indent, self.start))


        super(Fork, self).__init__(root)
        self.name= root.get('name')
        self.paths = [Path(elem) for elem in root]
            
            
    def dump(self, fs, indent):
        fs.write('{0}Fork: name -> {1}\n'.format(indent, self.name))
        for path in self.paths:
            path.dump(fs, indent + '   ')

    
class Kill(Node):
    def __init__(self, root):
        super(Kill, self).__init__(root)
        self.name = root.get('name')
        self.message = ''.join(root.itertext()).strip()

    def dump(self, fs, indent):
        fs.write('{0}Kill: name -> {1}\n'.format(indent, self.name))
        fs.write('   {0}Message: {1}\n'.format(indent, self.message))

    
class Decision(Node):
    def __init__(self, root):
        class Case(Node):
            def __init__(self, root):
                super(Case, self).__init__(root)
                self.to = root.get('to')
                self.cond = root.text.strip()
            
            def dump(self, fs, indent):
                fs.write('{0}Case: to -> {1}, cond -> {2}\n'.format(indent,
                                    self.to, self.cond))
                                    
        class Default(Node):
            def __init__(self, root):
                super(Default, self).__init__(root)
                self.to = root.get('to')
            
            def dump(self, fs, indent):
                fs.write('{0}Default: to -> {1}\n'.format(indent, self.to))
   
        super(Decision, self).__init__(root)
        self.name = root.get('name')
        self.cases = []
        for elem in root.find('{{{0}}}switch'.format(self.ns)):
            if elem.tag.endswith('case'):
                self.cases.append(Case(elem))
            else:
                self.cases.append(Default(elem))
            

    def dump(self, fs, indent):
        fs.write('{0}Decision: name -> {1}\n'.format(indent, self.name))
        for case in self.cases:
            case.dump(fs, '   ' + indent)


class Join(Node):

    def __init__(self, root):
        super(Join, self).__init__(root)
        self.name = root.get('name')
        self.to = root.get('to')


    def dump(self, fs, indent):
        fs.write('{0}Join: name -> {1}, to -> {2}\n'.format(indent, 
                                               self.name, self.to))


class Configuration(Node):
    def __init__(self, root):
        super(Configuration, self).__init__(root)
        self.props = []
        for elem in root:
            name = self.findelem(elem, 'name').text.strip()
            value = self.findelem(elem, 'value').text.strip()
            self.props.append((name, value))


    def dump(self, fs, indent):
        fs.write('   {0}Properties:\n'.format(indent))
        for name, value in self.props:
            fs.write('      {0}{1} = {2}\n'.format(indent, name, value))


class Shell(Node):
    def __init__(self, root):
        super(Shell, self).__init__(root)
        self.args = []
        self.config = None
        for elem in root:
            elem_type = self.remove_ns(elem.tag)
            if elem_type == 'job-tracker':
                self.jobtracker = elem.text.strip()
            elif elem_type == 'name-node':
                self.namenode = elem.text.strip()
            elif elem_type == 'exec':
                self.execfile = elem.text.strip()
            elif elem_type == 'exec':
                self.execfile = elem.text.strip()
            elif elem_type == 'env-var':
                self.env = elem.text.strip()
            elif elem_type == 'file':
                self.file = elem.text.strip()
            elif elem_type == 'argument':
                self.args.append(elem.text.strip())
            elif elem_type == 'configuration':
                self.config = Configuration(elem)


    def dump(self, fs, indent):
        fs.write('   {0}Shell: job-tracker -> {1}\n'.format(indent,
                                                            self.jobtracker))
        fs.write('   {0}       name-node -> {1}\n'.format(indent,
                                                          self.namenode))
        if self.config:
            self.config.dump(fs, '       ' + indent)
        fs.write('   {0}       exec -> {1}\n'.format(indent, self.execfile))

        if len(self.args) > 0:
            fs.write('   {0}       arguments -> [{1}]\n'.format(indent,
                                                          ' '.join(self.args)))
        if hasattr(self, 'env'):
            fs.write('   {0}       env-var -> {1}\n'.format(indent, self.env))
        if hasattr(self, 'file'):
            fs.write('   {0}       file -> {1}\n'.format(indent, self.file))



class Action(Node):
    def __init__(self, root):
        super(Action, self).__init__(root)
        self.action = None
        self.name = root.get('name')

        for elem in root:
            elem_type = self.remove_ns(elem.tag)
            if elem_type == 'ok':
                self.ok = elem.get('to')
            elif elem_type == 'error':
                self.error = elem.get('to')
            else:
                klass = {'shell' : Shell}.get(elem_type)
                if self.action is None:
                    self.action = klass(elem)
                else:
                    raise KeyError('Multiple actions in one action entry')
        

    def dump(self, fs, indent):
        fs.write('{0}Action: name -> {1}, ok -> {2}, error -> {3}\n'.format(
            indent, self.name, self.ok, self.error))
        self.action.dump(fs, indent)
    
    
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
    
class Sqoop(Action):
    pass