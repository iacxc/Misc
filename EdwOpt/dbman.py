
from __future__ import print_function

import threading
from _winreg import HKEY_CURRENT_USER, OpenKey, EnumValue
import wx


from controls import SqlEditor, DataGrid, ProgressStatusBar
from models import Seaquest
import resource as R


def get_connection(options):
    import odbc
    return odbc.get_connection(options)


def get_odbc_datasources():
    key = OpenKey(HKEY_CURRENT_USER, r'SOFTWARE\ODBC\ODBC.INI\ODBC DATA Sources')
    dsn_list = []
    index = 0
    try:
        while True:
            name, value, type = EnumValue(key, index)
            dsn_list.append(name)
            index += 1
    except WindowsError:
        pass

    return dsn_list


def create_button(parent, id, label, handler):
    button = wx.Button(parent, id, label)
    button.Bind(wx.EVT_BUTTON, handler)

    return button


class DataThread(threading.Thread):
    def __init__(self, window, func, *args):
        super(DataThread, self).__init__()

        self.daemon = True

        self.window = window
        self.func = func
        self.args = args

    def run(self):
        try:
            fields, rows = self.func(*self.args)
            wx.CallAfter(self.window.StopQuery, fields, rows)
        except Exception as exp:
            wx.MessageBox(str(exp), "Error fetching data", wx.OK)
            wx.CallAfter(self.window.StopQuery)


class DataFrame(wx.MDIChildFrame):
    """ """
    func_list = ('raw query',
                 'db::get_table_partitions',
                 'db::get_table_files',
                 'db::get_single_partition_objs',
                 'wms::status',
                 'wms::status_query',
                 'wms::status_service',
                 'wms::status_rule')

    def __init__(self, parent, title):
        super(DataFrame, self).__init__(parent, title=title)

        self.initUI()
        self.__conn = None

    @property
    def conn_option(self):
        class ConnOpt(object): pass

        opt = ConnOpt()
        opt.dsn = self.selDSN.GetValue()
        opt.user = self.txtUID.GetValue()
        opt.password = self.txtPWD.GetValue()

        return opt

    @property
    def connection(self):
        try:
            if self.__conn is None:
                self.__conn = get_connection(self.conn_option)
        except Exception as exp:
            wx.MessageBox(str(exp), "Error connecting to database", wx.OK)

        return self.__conn

    @property
    def database(self):
        if self.connection is None:
            return None
        else:
            return Seaquest.Database(self.connection, False)

    @property
    def wms(self):
        if self.connection is None:
            return None
        else:
            return Seaquest.WMSSystem(self.connection, False)

    def initUI(self):
        panel = wx.Panel(self)

        # controls
        self.selDSN = wx.ComboBox(panel, 0, "sqws114",
                                  choices=get_odbc_datasources())
        self.txtUID = wx.TextCtrl(panel, value=R.Value.DEF_USER)
        self.txtPWD = wx.TextCtrl(panel, value=R.Value.DEF_PASSWORD,
                                  style=wx.TE_PASSWORD)
        self.selFuncs = wx.ComboBox(panel, 0, self.func_list[0],
                                    choices=self.func_list)
        self.txtQuery = SqlEditor(panel)

        self.btnQuery = create_button(panel, R.Id.ID_QUERY,
                                      R.String.QUERY, self.OnBtnRun)
        self.btnWms = create_button(panel, R.Id.ID_WMS,
                                    R.String.WMS, self.OnBtnRun)

        self.datagrid = DataGrid(panel)

        # main sizer
        msizer = wx.BoxSizer(wx.VERTICAL)

        hbox1 = wx.StaticBoxSizer(
            wx.StaticBox(panel, label=R.String.LABEL_CONNINFO), wx.HORIZONTAL)

        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_DSN),
                  flag=wx.TOP|wx.RIGHT|wx.BOTTOM|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.selDSN, 1, wx.ALL, border=R.Value.BORDER)
        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_UID),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.txtUID, 2, wx.ALL, border=R.Value.BORDER)
        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_PWD),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.txtPWD, 2, wx.ALL, border=R.Value.BORDER)

        hbox2 = wx.BoxSizer(wx.HORIZONTAL)
        hbox2.Add(wx.StaticText(panel, label=R.String.LABEL_TESTFUNC),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL, border=R.Value.BORDER)
        hbox2.Add(self.selFuncs, flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)

        hbox3 = wx.BoxSizer(wx.HORIZONTAL)
        hbox3.Add(wx.StaticText(panel, label=R.String.LABEL_PARAMETERS),
                  flag=wx.ALL, border=R.Value.BORDER)
        hbox3.Add(self.txtQuery, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        btnsizer = wx.BoxSizer(wx.VERTICAL)

        btnsizer.Add(self.btnQuery, 1, wx.ALL, border=R.Value.BORDER)
        btnsizer.Add(self.btnWms, 1, wx.ALL, border=R.Value.BORDER)

        btnsizer.Add(create_button(panel, wx.ID_ANY, "Try", self.OnTry),
                     1, wx.ALL, border=R.Value.BORDER)

        hbox3.Add(btnsizer)

        gridbox = wx.StaticBoxSizer(
            wx.StaticBox(panel, label=R.String.LABEL_OUTPUT))
        gridbox.Add(self.datagrid, 1, wx.EXPAND)

        msizer.Add(hbox1, flag=wx.EXPAND|wx.ALL, border=R.Value.BORDER)
        msizer.Add(hbox2, flag=wx.EXPAND|wx.ALL, border=R.Value.BORDER)
        msizer.Add(hbox3, flag=wx.EXPAND|wx.ALL, border=R.Value.BORDER)
        msizer.Add(gridbox, 1, flag=wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        panel.SetSizer(msizer)

        self.selDSN.Bind(wx.EVT_COMBOBOX, self.OnSelDsnClicked)
        self.selFuncs.Bind(wx.EVT_COMBOBOX, self.OnSelFuncClicked)

        self.statusbar = ProgressStatusBar(self)
        self.SetStatusBar(self.statusbar)

    def UpdateStatus(self, msg):
        self.SetStatusText(msg)

    def StartQuery(self):
        self.statusbar.StartBusy()

    def StopQuery(self, fields=None, rows=None):
        self.UpdateStatus('Done')
        if fields is not None:
            self.datagrid.RefreshData(fields, rows)
        self.statusbar.StopBusy()

    def OnSelDsnClicked(self, event):
        self.__conn = None

    def OnSelFuncClicked(self, event):
        func_name = self.selFuncs.GetValue()
        if func_name.startswith('db::'):
            self.btnQuery.Enable()
            self.btnWms.Disable()
        elif func_name.startswith('wms::'):
            self.btnQuery.Disable()
            self.btnWms.Enable()
        else:
            self.btnQuery.Enable()
            self.btnWms.Enable()

    def OnBtnRun(self, event):
        event_id = event.GetId()
        self.StartQuery()

        self.UpdateStatus('Connecting ...')
        if event_id == R.Id.ID_QUERY:
            dbobj = self.database
        else: # R.Id.ID_WMS
            dbobj = self.wms

        if dbobj is None:
            self.StopQuery()
            return

        self.UpdateStatus('Querying ...')
        func_name = self.selFuncs.GetValue()
        if '::' in func_name:
            func_name = func_name.split('::')[1]
            func = getattr(dbobj, func_name)
            params = self.txtQuery.GetValue().strip().split()
            task = DataThread(self, func, *params)

        else:
            query = self.txtQuery.GetValue().strip()
            task = DataThread(self, dbobj.getall, query)

        task.start()

    def OnTry(self, event):
        self.statusbar.StartBusy()

class MainFrame(wx.MDIParentFrame):
    """ """
    def __init__(self, parent=None, title="", size=(1200,900)):
        super(MainFrame, self).__init__(parent, title=title, size=size)

        self.index = 0
        self.initUI()
        self.Center()

        self.Bind(wx.EVT_MENU, self.OnMenuClick)

    def initUI(self):
        menu = wx.Menu()
        menu.Append(R.Id.ID_DATAVIEW, R.String.MENU_DATAVIEW)
        menu.AppendSeparator()
        menu.Append(wx.ID_EXIT, R.String.MENU_EXIT)

        menubar = wx.MenuBar()
        menubar.Append(menu, R.String.MENU_TOOL)

        self.SetMenuBar(menubar)

    def OnMenuClick(self, event):
        event_id = event.GetId()
        if event_id == wx.ID_EXIT:
            self.Close(True)
        elif event_id == R.Id.ID_DATAVIEW:
            frame = DataFrame(self,
                              R.String.TITLE_DATAVIEW.format(self.index))
            self.index += 1
            frame.Show()



class MainApp(wx.App):
    def OnInit(self):
        self.frame = MainFrame(title=R.String.TITLE_DBMAN)
        self.frame.Show()

        return True


if __name__ == "__main__":
    app = MainApp()
    app.MainLoop()