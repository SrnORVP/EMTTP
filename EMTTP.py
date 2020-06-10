import pandas as pd, numpy as np, main_pandas, os, json

pd.set_option("display.max_columns", 20, "display.max_colwidth", 20, 'display.width', 100)

arrProjPath = main_pandas.arrProjPath
arrParaPath = main_pandas.arrParaPath
strEMTTP_Input = main_pandas.strEMTTP_Input
strEMTTP_Output = main_pandas.strEMTTP_Output
strEMTTP_Param = main_pandas.strEMTTP_Param
strEMTTP_Pjson = main_pandas.strEMTTP_Pjson


# Import Excel input and params
dictExcel = pd.read_excel(os.path.join(*arrProjPath, strEMTTP_Input), sheet_name=None)
dfParam_grp = pd.read_excel(os.path.join(*arrParaPath, strEMTTP_Param), sheet_name=0).groupby('strTab')
dfTrans_grp = pd.read_excel(os.path.join(*arrParaPath, strEMTTP_Param), sheet_name=1).groupby('strTab')

# Import json params
with open(os.path.join(*arrParaPath, strEMTTP_Pjson)) as fileJSON:
    dictParam = json.load(fileJSON)


# Get dict of columns for rename and reindex for each event
dictParam_dictColSel = dict()
for strTab, dfColParam in dfParam_grp:
    dictParam_dictColSel.update(dfColParam.pivot(index='strTab', columns='strCol', values='strIdx').to_dict('index'))
dictParam_dictTrnsFm = dict()
for strTab, dfTrnsParam in dfTrans_grp:
    dictParam_dictTrnsFm.update(dfTrnsParam.pivot(index='strTab', columns='strTar', values='strTrans').to_dict('index'))


# Rename and reindex DataFrame for each tab
# Pivot long on DataFrame for each event based on param in json
# Force numeric on 'value' col
dictExcel_Long = dict()
for strTab, dictColSel in dictParam_dictColSel.items():
    dfMelt = dictExcel[strTab].reindex(columns=[*dictColSel.keys()]).rename(columns=dictColSel)
    dfMelt = dfMelt.melt(id_vars=dictParam['melt_Index'], var_name=dictParam['melt_Col_Name'])
    if dictParam['isForceNumeric']:
        dfMelt['value'] = pd.to_numeric(dfMelt['value'], errors='coerce')
    dictExcel_Long[strTab] = dfMelt


# Expand the melt_Col_Name column 
for strTab, dfMerge in dictExcel_Long.items():
    arrExpandCol = dictParam['melt_Col_Name'].split('@@')
    dictRename = dict(zip(range(len(arrExpandCol)), arrExpandCol))
    dfExpandCol = dfMerge[dictParam['melt_Col_Name']]
    dfExpandCol = dfExpandCol.str.split('@@', expand=True).rename(columns=dictRename)
    dfMerge = dfMerge.merge(dfExpandCol, left_index=True, right_index=True).drop(columns=dictParam['melt_Col_Name'])
    dictExcel_Long[strTab] = dfMerge


# Parse columns
arrAllCol = dictParam['pivot_Index'] + dictParam['pivot_Cols']
arrNoValueCol = arrAllCol.copy()
arrNoValueCol.remove(arrExpandCol[-1])
strValueCol = arrExpandCol[-1]


def Pivot_on_Config(dictInput_dfTabs, arrPivotIndex, arrPivotCols, strFillna=None, isDataBase=False):
    dictOutput_dfTabs = dict()
    for strTab, dfTabs in dictInput_dfTabs.items():
        dfTabs = dfTabs.sort_values(arrPivotIndex + arrPivotCols)
        if arrPivotCols:
            dfTabs = pd.pivot_table(dfTabs, index=arrPivotIndex, columns=arrPivotCols, values='value',
                                    fill_value=strFillna, dropna=False)
        if isDataBase:
            dfTabs = dfTabs.reset_index()
        dictOutput_dfTabs[strTab] = dfTabs
    return dictOutput_dfTabs


# Pivot Wide based on User Configuration
dictExcel_RawParam = Pivot_on_Config(dictExcel_Long, dictParam['pivot_Index'], dictParam['pivot_Cols'],
                                     dictParam['pivot_fillna'], dictParam['isDataBase'])

# Transform Value based on formula
dictExcel_Trnsfm = dict()
if dictParam['isTransform']:
    for strTab, dfLong in dictExcel_Long.items():
        dfLong = pd.pivot_table(dfLong, index=["Scen", "Hole", "EndPoint","Weat"], columns='DistType', values='value',
                                fill_value=dictParam['pivot_fillna'], dropna=False)
        for strTar, strTrns in dictParam_dictTrnsFm[strTab].items():
            dfLong[strTar] = dfLong.apply(lambda x: eval(str(strTrns), {'x': x}), axis=1)
        dfLong = dfLong.reset_index().reindex(columns=dictParam['transform_Select_Cols'])
        dfLong = dfLong.melt(id_vars=arrNoValueCol, var_name=strValueCol)
        dictExcel_Trnsfm[strTab] = dfLong

    # Pivot Wide based on User Configuration
    dictExcel_Trnsfm = Pivot_on_Config(dictExcel_Trnsfm, dictParam['pivot_Index'], dictParam['pivot_Cols'],
                                         dictParam['pivot_fillna'], dictParam['isDataBase'])


# Export
with pd.ExcelWriter(os.path.join(*arrProjPath, strEMTTP_Output), 'openpyxl') as xwr:
    for strTab, df in dictExcel_RawParam.items():
        if dictParam['isExportRaw']:
            df.to_excel(xwr, strTab+'_raw')
        if dictParam['isTransform']:
            dictExcel_Trnsfm[strTab].to_excel(xwr, strTab+'_tfd')
