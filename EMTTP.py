import pandas as pd, numpy as np, main_pandas, os, json
# from tqdm import tqdm
# tqdm.pandas()

pd.set_option("display.max_columns", 20, "display.max_colwidth", 50, 'display.width', 200)

arrProjPath = main_pandas.arrProjPath
arrParaPath = main_pandas.arrParaPath
strEMTTP_Input = main_pandas.strEMTTP_Input
strEMTTP_Output = main_pandas.strEMTTP_Output
strEMTTP_Param = main_pandas.strEMTTP_Param
strEMTTP_Pjson = main_pandas.strEMTTP_Pjson


# Import json params
with open(os.path.join(*arrParaPath, strEMTTP_Pjson)) as fileJSON:
    dictParam = json.load(fileJSON)

# Import Excel input and params
dictExcel = pd.read_excel(os.path.join(*arrProjPath, strEMTTP_Input), sheet_name=None)
dfParam_grp = pd.read_excel(os.path.join(*arrParaPath, strEMTTP_Param), sheet_name=0).groupby('strTab')
dfTrans_grp = pd.read_excel(os.path.join(*arrParaPath, strEMTTP_Param), sheet_name=1).groupby('strTab')

# remove exempted and empty tabs
# print(dictExcel.keys())
arrRemovedTabs = [strTab for strTab in dictExcel.keys() if dictExcel[strTab].empty] + dictParam['bypassed_tabs']
[dictExcel.pop(strRemovedTabs, None) for strRemovedTabs in arrRemovedTabs]
# print(dictExcel.keys())

# For each excel tab, get the dict for renaming and reindexing
dictTab_dictColSel = dict()
for strTab, dfColParam in dfParam_grp:
    dictTab_dictColSel.update(dfColParam.pivot(index='strTab', columns='strCol', values='strIdx').to_dict('index'))

# For each excel tab, get the dict for filling empty input
dictTab_dictEmptyInput = dict()
for strTab, dfColParam in dfParam_grp:
    dictTab_dictEmptyInput.update(dfColParam.pivot(index='strTab', columns='strIdx', values='strFill').to_dict('index'))

# For each excel tab, get the dict for transformation of Param1, Param2 etc as defined
dictTab_dictTrnsFm = dict()
for strTab, dfTrnsParam in dfTrans_grp:
    dictTab_dictTrnsFm.update(dfTrnsParam.pivot(index='strTab', columns='strTar', values='strTrans').to_dict('index'))


# Rename and reindex DataFrame for each tab
# Pivot to long format for each tab based on 'melt_Index' and 'melt_Col_Name', as defined in json
# Fill NA on input
# Optional, force numeric on 'value' col
dictExcel_Long = dict()
for strTab, dfExcelTab in dictExcel.items():
    dfMelt = dfExcelTab.reindex(columns=[*dictTab_dictColSel[strTab].keys()]).rename(columns=dictTab_dictColSel[strTab])
    dfMelt = dfMelt.fillna(value=dictTab_dictEmptyInput[strTab])
    dfMelt = dfMelt.melt(id_vars=dictParam['melt_Index'], var_name=dictParam['melt_Col_Name'])
    if dictParam['isForceNumeric']:
        dfMelt['value'] = pd.to_numeric(dfMelt['value'], errors='coerce')
    dictExcel_Long[strTab] = dfMelt

# Expand the melt_Col_Name columns and rename as required
arrAllCol = dictParam['pivot_Index'] + dictParam['pivot_Cols']
arrCol_Variables = arrAllCol.copy()
if '@@' in dictParam['melt_Col_Name']:
    for strTab, dfMerge in dictExcel_Long.items():
        arrSplitColNames = dictParam['melt_Col_Name'].split('@@')
        dictSplitColRename = dict(zip(range(len(arrSplitColNames)), arrSplitColNames))
        dfExpandCol = dfMerge[dictParam['melt_Col_Name']]
        dfExpandCol = dfExpandCol.str.split('@@', expand=True).rename(columns=dictSplitColRename)
        dfMerge = dfMerge.merge(dfExpandCol, left_index=True, right_index=True).drop(columns=dictParam['melt_Col_Name'])
        dictExcel_Long[strTab] = dfMerge
        # print(dfMerge)

    # print(arrSplitColNames)
    arrCol_Variables.remove(arrSplitColNames[-1])
    # print(arrCol_Variables)
    strColName_Value = arrSplitColNames[-1]
    # print(strColName_Value)
else:
    arrCol_Variables.remove(dictParam['melt_Col_Name'])
    # print(arrCol_Variables)
    strColName_Value = dictParam['melt_Col_Name']
    # print(strColName_Value)


def Pivot_on_Config(dictInput_dfTabs, arrPivotIndex, arrPivotCols, strFillna=None, isDataBase=False, isRemoveNull=True):
    dictOutput_dfTabs = dict()
    for strTab, dfTabs in dictInput_dfTabs.items():
        dfTabs = dfTabs.sort_values(arrPivotIndex + arrPivotCols)
        if arrPivotCols:
            dfTabs = pd.pivot_table(dfTabs, index=arrPivotIndex, columns=arrPivotCols, values='value',
                                    fill_value=strFillna, dropna=isRemoveNull)
        if isDataBase:
            dfTabs = dfTabs.reset_index()
        dictOutput_dfTabs[strTab] = dfTabs
    return dictOutput_dfTabs


# print(dictExcel_Long.values())
# print(arrCol_Variables)
# print(strColName_Value)

# Transform Value based on formula
if dictParam['isTransform']:
    for strTab, dfLong in dictExcel_Long.items():
        dfLong = pd.pivot_table(dfLong, index=arrCol_Variables, columns=strColName_Value, values='value',
                                fill_value=dictParam['pivot_FillEmptyValue'], dropna=True)
        for strTar, strTrns in dictTab_dictTrnsFm[strTab].items():
            dfLong[strTar] = dfLong.apply(lambda x: eval(str(strTrns), {'x': x, "__builtins__": {}}), axis=1)
        dfLong = dfLong.reset_index().reindex(columns=dictParam['transform_Select_Cols'])
        dfLong = dfLong.melt(id_vars=arrCol_Variables, var_name=strColName_Value)
        dictExcel_Long[strTab] = dfLong

# Pivot Wide based on User Configuration
dictExcel_Long = Pivot_on_Config(dictExcel_Long, dictParam['pivot_Index'], dictParam['pivot_Cols'],
                                 dictParam['pivot_FillEmptyValue'], dictParam['isDataBase'], dictParam['isRemoveNull'])


# Export
with pd.ExcelWriter(os.path.join(*arrProjPath, strEMTTP_Output), 'openpyxl') as xwr:
    for strTab, df in dictExcel_Long.items():
        df.to_excel(xwr, strTab)
