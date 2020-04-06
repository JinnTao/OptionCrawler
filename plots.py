import os
import sys
from PyQt4 import QtCore, QtGui
from PyQt4.QtGui import *
from PyQt4.QtCore import pyqtSignal, Qt, QVariant, QObject

from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas


import matplotlib.ticker
class DateTimeFormatter(matplotlib.ticker.Formatter):
    def __init__(self, dates, fmt='%Y-%m-%d %H:%M'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(round(x))
        if ind >= len(self.dates) or ind < 0:
            return ''

        return self.dates[ind].strftime(self.fmt)

class FigureModel(QObject):
    dataChanged = pyqtSignal()
    def __init__(self):
        super(FigureModel,self).__init__()

    def setupFigure(self, ax, ):
        pass

    def numAxes(self):
        '''Return number of Axes'''
        return 0

    def plot(self, index, ax):
        pass

    def setup(self, index, ax):
        pass

class MplCanvas(FigureCanvas):
    """Ultimately, this is a QWidget (as well as a FigureCanvasAgg, etc.)."""

    def __init__(self, parent=None, width=10, height=8, dpi=600):
        from matplotlib.figure import Figure
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.fig = fig
        self.axes = fig.add_subplot(111)
        # We want the axes cleared every time plot() is called
        self.axes.hold(False)

        self.compute_initial_figure()

        #
        FigureCanvas.__init__(self, fig)
        self.setParent(parent)

        FigureCanvas.setSizePolicy(self,
                                   QtGui.QSizePolicy.Expanding,
                                   QtGui.QSizePolicy.Expanding)
        FigureCanvas.updateGeometry(self)

    def reset(self):
        self.fig.clf()
        self.axes = self.fig.add_subplot(111)
        self.axes.hold(False)

    def compute_initial_figure(self):
        pass

class FigureView(MplCanvas):
    '''Model-View style implementation of a matplotlib plot'''
    def __init__(self, parent, model, *args, **kwargs):
        kwargs['parent']=parent
        super(FigureView,self).__init__(*args,**kwargs)
        self.ax_list = []
        self.model = None
        self.setModel(model)

    def setModel(self, model):
        if self.model:
            self.model.dataChanged.disconnect(self.update_figure)
            self.reset()
            self.ax_list = []
        self.model = model
        if self.model:
            model.setupFigure(self.fig)
            for i in range(model.numAxes()):
                if i == 0:
                    ax = self.axes
                else:
                    ax = self.axes.twinx()
                ax.hold(False)
                self.ax_list.append(ax)
                if i == 2:
                    ax.spines['right'].set_position(('axes', 1.2))
                model.setup(i, ax)
            self.model.dataChanged.connect(self.update_figure)
        self.update_figure()

    def add_data(self, x, y):
        ax = self.axes.twinx()
        ax.hold(False)
        self.ax_list.append(ax)

    def update_figure(self):
        for i in range(len(self.ax_list)):
            self.model.plot(i, self.ax_list[i])

        handles,labels = [],[]
        for ax in self.ax_list:
            for h,l in zip(*ax.get_legend_handles_labels()):
                handles.append(h)
                labels.append(l)

        if self.ax_list:
            kwargs = dict(loc="lower right", bbox_to_anchor=(1,0))
            self.ax_list[-1].legend(handles, labels, **kwargs)
        self.draw()


if __name__ == '__main__':
    import random
    from numpy import arange, sin, pi

    class MyFigureModel(FigureModel):
        def __init__(self):
            super(MyFigureModel, self).__init__()
            count = 20
            self.x = arange(0,count,1)
            self.data = [[random.randint(0, 10) for j in range(count)] for i in range(3)]
            self.color = ['r','g','b']

        def numAxes(self):
            return 3

        def plot(self, i, ax):
            if i==0:
                ax.plot( self.x, self.data[i], color=self.color[i], label=str(i) )
                ax.grid(True)
            if i==1:
                ax.bar( self.x, self.data[i], color=self.color[i], label=str(i) )
            if i == 2:
                ax.plot( self.x, self.data[i], color=self.color[i], label=str(i) )
                ax.spines['right'].set_position(('axes', 1.1))
                ax.set_frame_on(True)
                ax.patch.set_visible(False)
                ax.yaxis.tick_right()
                ax.tick_params(axis = 'y', direction='out')
                ax.margins(0,0.05)

        def setup(self, i, ax):
            pass

        def setupFigure(self, fig):
            fig.subplots_adjust(right=0.8)

        def updateData(self):
            count = 20
            self.data[2] = [random.randint(0, 10) for j in range(count)]
            self.dataChanged.emit()

    class ApplicationWindow(QtGui.QMainWindow):
        def __init__(self):
            QtGui.QMainWindow.__init__(self)
            self.setAttribute(QtCore.Qt.WA_DeleteOnClose)
            self.setWindowTitle("application main window")

            self.file_menu = QtGui.QMenu('&File', self)
            self.file_menu.addAction('&Quit', self.fileQuit,
                                     QtCore.Qt.CTRL + QtCore.Qt.Key_Q)
            self.menuBar().addMenu(self.file_menu)

            self.help_menu = QtGui.QMenu('&Help', self)
            self.menuBar().addSeparator()
            self.menuBar().addMenu(self.help_menu)

            self.help_menu.addAction('&About', self.about)

            self.main_widget = QtGui.QWidget(self)

            l = QtGui.QVBoxLayout(self.main_widget)
            m = MyFigureModel()
            fv = FigureView(self.main_widget, m, width=5, height=4, dpi=None)
            btn = QPushButton("Change", self.main_widget)
            rmbtn = QPushButton("Remove", self.main_widget)
            adbtn = QPushButton("Add", self.main_widget)
            l.addWidget(fv)
            l.addWidget(btn)
            l.addWidget(rmbtn)
            l.addWidget(adbtn)

            btn.clicked.connect(m.updateData)
            rmbtn.clicked.connect(lambda: fv.setModel(None))
            adbtn.clicked.connect(lambda: fv.setModel(m))

            self.main_widget.setFocus()
            self.setCentralWidget(self.main_widget)

            self.statusBar().showMessage("All hail matplotlib!", 2000)

        def fileQuit(self):
            self.close()

        def closeEvent(self, ce):
            self.fileQuit()

        def about(self):
            QtGui.QMessageBox.about(self, "About",
                                """embedding_in_qt4.py example
Copyright 2005 Florent Rougon, 2006 Darren Dale

This program is a simple example of a Qt4 application embedding matplotlib
canvases.

It may be used and modified with no restriction; raw copies as well as
modified versions may be distributed without limitation."""
                                )

    import sys
    qApp = QtGui.QApplication(sys.argv)

    progname = os.path.basename(sys.argv[0])
    aw = ApplicationWindow()
    aw.setWindowTitle("%s" % progname)
    aw.show()
    sys.exit(qApp.exec_())
