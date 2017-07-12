
from __future__ import print_function

import wx

from _winreg import HKEY_CURRENT_USER, OpenKey, EnumValue
from controls import SqlEditor, DataGrid
from models import Seaquest
import resource as R


class ConnOpt(object):
    def __init__(self, dsn, uid, pwd):
        self.dsn = dsn
        self.user = uid
        self.password = pwd


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


def create_button(parent, label, handler):
    button = wx.Button(parent, label=label)
    button.Bind(wx.EVT_BUTTON, handler)

    return button



class DataFrame(wx.MDIChildFrame):
    """ """
    func_list = ('raw query',
                 'db:get_table_partitions',
                 'db:get_table_files',
                 'db:get_single_partition_objs',
                 'wms:status', 'wms:status_query',
                 'wms:status_service', 'wms:status_rule')

    def __init__(self, parent, title):
        super(DataFrame, self).__init__(parent, title=title)

        self.initUI()

    def initUI(self):
        panel = wx.Panel(self)

        self.selDSN = wx.ComboBox(panel, -1, 'sqws114',
                                  choices=get_odbc_datasources())
        self.txtUID = wx.TextCtrl(panel, value=R.Value.DEF_USER)
        self.txtPWD = wx.TextCtrl(panel, value=R.Value.DEF_PASSWORD)
        self.selFuncs = wx.ComboBox(panel, -1, self.func_list[0],
                                    choices=self.func_list,
                                    style=wx.CB_DROPDOWN)
        self.txtParams = wx.TextCtrl(panel)
        self.txtQuery = SqlEditor(panel)

        self.datagrid = DataGrid(panel)

        self.btnRun = create_button(panel, R.String.QUERY, self.OnBtnRun)
        self.btnWms = create_button(panel, R.String.WMS, self.OnBtnWms)

        # main sizer
        msizer = wx.BoxSizer(wx.VERTICAL)

        hbox1 = wx.BoxSizer(wx.HORIZONTAL)

        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_DSN),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.selDSN, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_UID),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.txtUID, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)
        hbox1.Add(wx.StaticText(panel, label=R.String.LABEL_PWD),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox1.Add(self.txtPWD, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        hbox2 = wx.BoxSizer(wx.HORIZONTAL)
        hbox2.Add(wx.StaticText(panel, label=R.String.LABEL_TESTFUNC),
                  flag=wx.ALL, border=R.Value.BORDER)
        hbox2.Add(self.selFuncs, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox2.Add(wx.StaticText(panel, label=R.String.LABEL_PARAMETERS),
                  flag=wx.ALL|wx.ALIGN_CENTER_VERTICAL,
                  border=R.Value.BORDER)
        hbox2.Add(self.txtParams, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        hbox3 = wx.BoxSizer(wx.HORIZONTAL)
        hbox3.Add(wx.StaticText(panel, label=R.String.LABEL_QUERY),
                  flag=wx.ALL, border=R.Value.BORDER)
        hbox3.Add(self.txtQuery, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        btnsizer = wx.BoxSizer(wx.VERTICAL)

        btnsizer.Add(self.btnRun, 0, wx.ALL, border=R.Value.BORDER)
        btnsizer.Add(self.btnWms, 0, wx.ALL, border=R.Value.BORDER)

        hbox3.Add(btnsizer)

        gridbox = wx.StaticBoxSizer(wx.StaticBox(panel))
        gridbox.Add(self.datagrid, 1, wx.EXPAND)

        msizer.Add(hbox1, 0, flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT,
                   border=R.Value.BORDER)
        msizer.Add(hbox2, 0, flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT,
                   border=R.Value.BORDER)
        msizer.Add(hbox3, 0, flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT,
                   border=R.Value.BORDER)
        msizer.Add(gridbox, 1, wx.EXPAND|wx.ALL, border=R.Value.BORDER)

        panel.SetSizer(msizer)

        self.selFuncs.Bind(wx.EVT_COMBOBOX, self.OnFuncClicked)

    def OnBtnRun(self, event):
        opts = ConnOpt(self.selDSN.GetValue(),
                       self.txtUID.GetValue(),
                       self.txtPWD.GetValue())
        try:
            db = Seaquest.Database(opts)
            func_name = self.selFuncs.GetValue()
            if func_name.startswith('db:'):
                func = getattr(db, func_name[3:])
                params = self.txtParams.GetValue().strip().split()
                self.datagrid.RefreshData(*func(*params))
            else:
                query = self.txtQuery.GetValue()
                self.datagrid.RefreshData(*db.getall(query))
        except Exception as exp:
            print(exp)

    def OnBtnWms(self, event):
        opts = ConnOpt(self.selDSN.GetValue(),
                       self.txtUID.GetValue(),
                       self.txtPWD.GetValue())
        try:
            wms = Seaquest.WMSSystem(opts, False)

            func_name = self.selFuncs.GetValue()
            if func_name.startswith('wms:'):
                func = getattr(wms, func_name[4:])
                params = self.txtParams.GetValue().strip().split()
                self.datagrid.RefreshData(*func(*params))
            else:
                query = self.txtQuery.GetValue()
                self.datagrid.RefreshData(*wms.getall(query))

        except Exception as exp:
            wx.MessageBox(str(exp), "Error", wx.OK)

    def OnFuncClicked(self, event):
        func_name = self.selFuncs.GetValue()
        if func_name.startswith('db'):
            self.btnRun.Enable()
            self.btnWms.Disable()
        elif func_name.startswith('wms'):
            self.btnRun.Disable()
            self.btnWms.Enable()
        else:
            self.btnRun.Enable()
            self.btnWms.Enable()
        pass

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