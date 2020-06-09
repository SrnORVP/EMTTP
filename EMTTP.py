import pandas as pd, numpy as np, main_pandas, os, json

pd.set_option("display.max_columns", 10, "display.max_colwidth", 20)

arrProjPath = main_pandas.arrProjPath
arrParaPath = main_pandas.arrParaPath
strEMTTP_Input = main_pandas.strEMTTP_Input
strEMTTP_Output = main_pandas.strEMTTP_Output
strEMTTP_Param = main_pandas.strEMTTP_Param
strEMTTP_Pjson = main_pandas.strEMTTP_Pjson


# Import Excel input and params
dictExcel = pd.read_excel(os.path.join(*arrProjPath, strEMTTP_Input), sheet_name=None)
dfParam_grp = pd.read_excel(os.path.join(*arrParaPath, strEMTTP_Param)).groupby('strTab')


# Import json params
with open(os.path.join(*arrParaPath, strEMTTP_Pjson)) as fileJSON:
    dictMetaParam = json.load(fileJSON)


# Get dict of columns for rename and reindex for each event
dictEvet_dictColSel = dict()
for strTab, dfEvetParam in dfParam_grp:
    dictEvet_dictColSel.update(dfEvetParam.pivot(index='strTab', columns='strCol', values='strIdx').to_dict('index'))


# Rename and reindex DataFrame for each event
dictExcel_ftd = dict()
for strTab, dictColSel in dictEvet_dictColSel.items():
    dfExcelTab = dictExcel[strTab].reindex(columns=[*dictColSel.keys()])
    dfExcelTab = dfExcelTab.rename(columns=dictColSel)
    dictExcel_ftd[strTab] = dfExcelTab


# Pivot long on DataFrame for each event based on param in json
dictExcel_Pivot = dict()
for strTab, dfTemp in dictExcel_ftd.items():
    dfTemp = dfTemp.melt(id_vars=dictMetaParam['long_Row'], var_name=dictMetaParam['long_Col'])
    dictExcel_Pivot[strTab] = dfTemp


# Expand the var_name column
for strTab, dfPivot in dictExcel_Pivot.items():
    arrColName = dictMetaParam['long_Col'].split('@@')
    dictRename = dict(zip(range(len(arrColName)), arrColName))
    dfTemp = dfPivot[dictMetaParam['long_Col']]
    dfTemp = dfTemp.str.split('@@', expand=True).rename(columns=dictRename)
    dfPivot = dfPivot.merge(dfTemp, left_index=True, right_index=True).drop(columns=dictMetaParam['long_Col'])
    dictExcel_Pivot[strTab] = dfPivot


# Pivot Wide based on User Configuration
for strTab, dfPivot in dictExcel_Pivot.items():
    dfTemp = dfPivot.set_index(dictMetaParam['wide_Idx'])
    dfTemp = dfTemp.sort_index()
    if dictMetaParam['wide_Col']:
        dfTemp = dfTemp.unstack(dictMetaParam['wide_Col'], fill_value=dictMetaParam['fill_n/a'])
        dfTemp.columns = dfTemp.columns.droplevel(0)
    if dictMetaParam['isDataBase']:
        dfTemp = dfTemp.reset_index()
    dictExcel_Pivot[strTab] = dfTemp


# Export
with pd.ExcelWriter(os.path.join(*arrProjPath, strEMTTP_Output), 'openpyxl') as xwr:
    for strTab, df in dictExcel_Pivot.items():
        df.to_excel(xwr, strTab)
