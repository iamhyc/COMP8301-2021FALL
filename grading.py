#!/usr/bin/env python3
import sys
from openpyxl import load_workbook
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (QApplication, QWidget, QLabel, QComboBox, QPushButton,
                                QLineEdit, QPlainTextEdit, QGridLayout)
from PyQt5.QtWidgets import (QListWidget)

FILENAME = './COMP8301_1A_2021_Grades.xlsx'
wb = load_workbook(filename=FILENAME)
wb_sheet = wb['Grades']

GET_FIRST_NAME = lambda x: wb_sheet[f'A{x}'].value
GET_LAST_NAME  = lambda x: wb_sheet[f'B{x}'].value
GET_ID_NUMBER  = lambda x: wb_sheet[f'C{x}'].value
GET_GROUP_NUM  = lambda x: int(wb_sheet[f'N{x}'].value)

IDX_RANGE = list( range(2,66) )
IDX_NAMES = ['(%02d) %s %s'%(GET_GROUP_NUM(x), GET_FIRST_NAME(x),GET_LAST_NAME(x)) for x in IDX_RANGE]

class AssignmentItem:
    def __init__(self, idx, g_sym, f_sym) -> None:
        self.idx = str(idx)
        self.g_sym = str(g_sym) + self.idx
        self.f_sym = str(f_sym) + self.idx
        pass

    @property
    def grade(self):
        return str(wb_sheet[ self.g_sym ].value)
    @grade.setter
    def grade(self, grade):
        try:
            wb_sheet[ self.g_sym ] = float(grade)
        except:
            wb_sheet[ self.g_sym ] = '-'

    @property
    def feedback(self):
        return wb_sheet[ self.f_sym ].value
    @feedback.setter
    def feedback(self, feedback):
        wb_sheet[ self.f_sym ] = str(feedback)

    pass

def Assignment(idx):
    if idx==1:
        g_sym, f_sym = 'G', 'H'
    elif idx==2:
        g_sym, f_sym = 'I', 'J'
    elif idx==3:
        g_sym, f_sym = 'K', 'L'
    else:
        raise Exception('wrong assignment index.')
    
    _map = dict()
    for idx in IDX_RANGE:
        _map[idx] = AssignmentItem(idx, g_sym, f_sym)
    return _map

EX_LIST = [ Assignment(1), Assignment(2), Assignment(3) ]

class KeysReactor():

    keySpecs = {
        Qt.Key_Control: 0x01,
        Qt.Key_Alt:     0x02,
        Qt.Key_Shift:   0x04
    }
    keySpecsKeys = keySpecs.keys()

    def __init__(self, parent, name='Default'):
        self.name = name
        self.key_list = [0x00]
        self.reactor  = dict()
        self.press_hook_pre    = None
        self.press_hook_post   = None
        self.release_hook_pre  = None
        self.release_hook_post = None

        if parent:
            self.parent = parent
            self.super  = super(type(parent), parent)
            parent.keyPressEvent   = lambda e: self.pressed(e.key(), e)
            parent.keyReleaseEvent = lambda e: self.released(e.key(), e)
        pass
    
    def __str__(self):
        ret = []
        ret.append('Ctrl')  if self.key_list[0]&0x01 else None
        ret.append('Alt')   if self.key_list[0]&0x02 else None
        ret.append('Shift') if self.key_list[0]&0x04 else None
        ret += [str(x) for x in self.key_list[1:]]
        return '[%s] %s'%(self.name, '+'.join(ret))
    
    def register(self, keys, hookfn):
        key_hash = [0x00]
        for tmp_key in keys:
            if tmp_key in self.keySpecsKeys: # for specific keys
                key_hash[0] = key_hash[0] | self.keySpecs[tmp_key]
            else:                            # for general keys
                key_hash.append(tmp_key)
            pass
        key_hash = '_'.join([str(x) for x in key_hash])
        self.reactor[key_hash] = hookfn
        pass
    
    def pressed(self, key, e=None):
        #NOTE: pre hook
        if self.press_hook_pre:
            self.press_hook_pre(e)

        #NOTE: press keys
        if key in self.keySpecsKeys:
            self.key_list[0] = self.key_list[0] | self.keySpecs[key] #append specific keys
        else:
            self.key_list.append(key)
        key_hash = '_'.join([str(x) for x in self.key_list])
        if key_hash in self.reactor:
            ret = self.reactor[key_hash]() #unused ret code
        else:
            self.super.keyPressEvent(e)
        
        #NOTE: post hook
        if self.press_hook_post:
            self.press_hook_post(e)
        pass
    
    def released(self, key, e=None):
        #NOTE: pre hook
        if self.release_hook_pre:
            self.release_hook_pre(e)
        
        #NOTE: remove keys
        if key in self.keySpecsKeys:                # remove a special key
            self.key_list[0] = self.key_list[0] & (~self.keySpecs[key]) #remove specific keys
        elif key in self.key_list:                  # remove a common key
            self.key_list.remove(key)
        elif key in [Qt.Key_Return, Qt.Key_Enter]:  # reset the list
            self.key_list = [0x00]
        self.super.keyReleaseEvent(e)
        if self.key_list[0]==0x00: #reset, if no specific keys presented
            self.key_list = [0x00]

        #NOTE: post hook
        if self.release_hook_post:
            self.release_hook_post(e)
        pass

    def hasSpecsKeys(self):
        return self.key_list[0] != 0x00

    def clear(self):
        self.key_list = [0x00]
        pass

    def setKeyPressHook(self, press_hook, mode='post'):
        assert( mode in ['pre','post','block'] )
        setattr(self, f'press_hook_{mode}', press_hook)
        pass

    def setKeyReleaseHook(self, release_hook, mode='post'):
        assert( mode in ['pre','post','block'] )
        setattr(self, f'release_hook_{mode}', release_hook)
        pass

    pass

class GradingWindow(QWidget):

    def __init__(self):
        super().__init__()
        self.grid = QGridLayout()
        
        # Row 0
        ## uid label
        self.uid_label = QLineEdit(parent=self)
        self.uid_label.setDisabled(True)
        self.grid.addWidget(self.uid_label, 0, 0, 1, 2)
        ## ex combobox
        self.ex_comb = QComboBox()
        self.ex_comb.addItems(['ex01', 'ex02', 'ex03'])
        self.ex_comb.currentIndexChanged.connect( self.update )
        self.grid.addWidget(self.ex_comb, 0, 2, 1, 1)
        ## name box
        self.name_box = QLineEdit(parent=self)
        # self.name_box.focusInEvent = lambda _: self.name_box.selectAll()
        self.name_box.textChanged.connect( self.update )
        self.grid.addWidget(self.name_box, 0, 3, 1, 2)

        ## Row 1 - 4
        ## feedback box
        self.feedback_box = QPlainTextEdit(self)
        self.grid.addWidget(self.feedback_box, 1, 0, 4, 3)
        ## name list
        self.name_list = QListWidget(self)
        self.name_list.addItems(IDX_NAMES)
        self.name_list.currentRowChanged.connect( self.update_by_selection )
        self.grid.addWidget(self.name_list, 1, 3, 4, 2)

        ## Row 5
        self.grade_box = QLineEdit(self)
        self.grade_box.mousePressEvent = lambda _:self.grade_box.selectAll()
        self.grid.addWidget(self.grade_box, 5, 0, 1, 3)
        #
        self.save_btn = QPushButton('Save', self)
        self.save_btn.clicked.connect( self.save )
        self.grid.addWidget(self.save_btn, 5, 3, 1, 2)

        # init process
        self.init()
        #
        self.keysFn = KeysReactor(self, 'GradingWindow')
        self.keysFn.register([Qt.Key_Control, Qt.Key_L], lambda:self.name_box.setFocus())
        self.keysFn.register([Qt.Key_Control, Qt.Key_G], lambda:self.grade_box.setFocus())
        self.keysFn.register([Qt.Key_Control, Qt.Key_S], lambda:self.save_btn.click())
        #
        self.setLayout(self.grid)
        self.resize(self.sizeHint())
        self.setWindowTitle('Grading Window')
        self.setAttribute(Qt.WA_InputMethodEnabled)
        self.show()
        pass

    def init(self):
        self.idx = IDX_RANGE[0]
        self.update_by_idx(self.idx)
        pass

    def search(self, name:str):
        _name = name.lower()
        for i, x in enumerate(IDX_NAMES):
            if _name in x.lower():
                return IDX_RANGE[i]
            pass
        return -1

    def update_by_idx(self, idx):
        _name = '%s %s'%( GET_FIRST_NAME(idx), GET_LAST_NAME(idx) )
        _uid  = GET_ID_NUMBER(idx)
        _ex_idx = self.ex_comb.currentIndex()
        _ex_ref = EX_LIST[ _ex_idx ]
        #
        self.uid_label.setText(f'{_name}, {_uid}')
        self.grade_box.setText( _ex_ref[idx].grade )
        self.feedback_box.setPlainText( _ex_ref[idx].feedback )
        pass

    def update(self, *args, **kwargs):
        _name = self.name_box.text()
        _index = self.search(_name)
        if _index>=0:
            self.idx = _index
            self.name_list.setCurrentRow( IDX_RANGE.index(_index) )
            self.update_by_idx(self.idx)
        pass

    def update_by_selection(self, *args, **kwargs):
        _index = IDX_RANGE[ self.name_list.currentRow() ]
        self.idx = _index
        self.update_by_idx(self.idx)

    def save(self):
        _ex_idx = self.ex_comb.currentIndex()
        _ex_ref = EX_LIST[ _ex_idx ]
        #
        _ex_ref[ self.idx ].grade = self.grade_box.text()
        _ex_ref[ self.idx ].feedback = self.feedback_box.toPlainText()
        wb.save(filename=FILENAME)
        #
        self.update_by_idx( self.idx )
        pass

    pass

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = GradingWindow()
    sys.exit(app.exec_())
