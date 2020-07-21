import sys, os

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
# General Project Name
strProjID = 'Phast67'

# Relative path and File name of user input file
arrProjPath = ['..', '01-Test']
strEMTTP_Input = strProjID + '-QRA' + '.xlsx'

# Name of Output Identifier
strEMTTP_Output = strProjID + '-EMTTP Output' + '.xlsx'

# Relative path and File name of General Param Inputs
strParaID = strProjID
arrParaPath = ['.', 'Param-'+strParaID]
strEMTTP_Param = strParaID + '-EMTTP' + '.xlsx'
strEMTTP_Pjson = strParaID + '-EMTTP' + '.json'

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    sys.path.append(os.getcwd())
    arrPathShort = os.getcwd().split(os.path.sep)[-1:]
    intUI = int(input(f'Which script you would like to run? [{strParaID}=1]: '))
    if intUI == 1:
        strPath_Script = os.path.join(*arrPathShort, 'EMTTP.py')
        print(f'\n"{strPath_Script}" is running on "{os.path.join(arrProjPath[-1], strEMTTP_Input)}".\n')
        import EMTTP
        EMTTP.__name__
        print(f'\n"{strPath_Script}" has ran successfully.')
        print(f'Output is saved in "{os.path.join(arrProjPath[-1], strEMTTP_Output)}".\n')
    elif intUI == 2:
        pass
    elif intUI == 3:
        pass
    elif intUI == 4:
        pass
    else:
        print('Invalid Input: Script Exit.')
        input('Press any key to exit.')
        exit()

    input('Press any key to exit.')

