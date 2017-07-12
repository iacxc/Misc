

import wx
import wx.grid
import wx.stc


class SqlEditor(wx.stc.StyledTextCtrl):
    def __init__(self, parent, value=None):
        super(SqlEditor, self).__init__(parent, size=(300, 180))

        self.SetLexer(wx.stc.STC_LEX_SQL)
        self.SetKeyWords(0,
            " ".join([ 'select', 'from', 'insert', 'where',
                       'order', 'group', 'by',
                       'status', 'wms', 'query', 'queries', 'all',
                       'service', 'rule']))

        self.SetupStyles()
        self.EnableLineNumber()

        if value:
            self.SetText(value)


    def EnableLineNumber(self):
        #enable line number margin
        self.SetMarginType(1, wx.stc.STC_MARGIN_NUMBER)
        self.SetMarginMask(1,0)
        self.SetMarginWidth(1, 25)


    def SetupStyles(self):

        default = "fore:#000000"
        self.StyleSetSpec(wx.stc.STC_STYLE_DEFAULT, default)

        line = "back:#C0C0C0"
        self.StyleSetSpec(wx.stc.STC_STYLE_LINENUMBER, line)

        self.StyleSetSpec(wx.stc.STC_SQL_DEFAULT, default)

        self.StyleSetSpec(wx.stc.STC_SQL_COMMENT, "fore:#007F00")
        self.StyleSetSpec(wx.stc.STC_SQL_NUMBER, "fore:#007F7F")
        self.StyleSetSpec(wx.stc.STC_SQL_STRING, "fore:#7F007F")
        self.StyleSetSpec(wx.stc.STC_SQL_WORD, "fore:#7F0000,bold")
        self.StyleSetSpec(wx.stc.STC_SQL_OPERATOR, "bold")


class DataGrid(wx.grid.Grid):
    def __init__(self, parent):
        super(DataGrid, self).__init__(parent)
        self.CreateGrid(0, 0)

    def RefreshData(self, titles, rows):
        self.BeginBatch()

        # clear all
        col_number = self.GetNumberCols()
        row_number = self.GetNumberRows()
        if col_number > 0:
            self.DeleteCols(0, col_number)

        if row_number > 0:
            self.DeleteRows(0, row_number)

        self.AppendCols(len(titles))
        for index, title in enumerate(titles):
            self.SetColLabelValue(index, title)
            self.AutoSizeColumn(index)

        self.AppendRows(len(rows))
        for rindex, row in enumerate(rows):
            for cindex, value in enumerate(row):
                self.SetCellValue(rindex, cindex, str(value))
                self.AutoSizeColumn(cindex)

        self.EndBatch()

        self.ForceRefresh()

