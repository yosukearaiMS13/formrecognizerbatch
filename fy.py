# Copyright © Microsoft Corporation All rights reserved.
#
# History
## 28th Aug. 2020 1st release (beta 1)
#
########### Python Form Recognizer Async Analyze #############
import json
import time
import datetime
import csv
import glob
import os

from requests import get, post

## Configurations
endpoint = r"https://xxxxx.cognitiveservices.azure.com/"
apim_key = "xxxxx"
model_id = "xxxxx"
sourceDir = r"C:\xxxxx\*"
confidence_setting = 0.9 # 0~1. 信頼性がこの値以下の場合採用しない
##

post_url = endpoint + "/formrecognizer/v2.0/custom/models/%s/analyze" % model_id
params = {
    "includeTextDetails": True
}

headers = {
     # Request headers
    'Content-Type': 'application/pdf',
    'Ocp-Apim-Subscription-Key': apim_key,
}

########### Post 分析対象pdf section #############

# 複数ファイル対応
getUrls_dict = {} # {filename, 分析結果取得url}

for file in glob.glob(sourceDir):

	with open(file, "rb") as f:
		data_bytes = f.read()

	try:
		resp = post(url = post_url, data = data_bytes, headers = headers, params = params)
		if resp.status_code != 202:
			print("POST analyze failed:\n%s" % json.dumps(resp.json()))
			quit()
		getUrls_dict[os.path.split(file)[1]] = resp.headers["operation-location"]
		print("POST analyze succeeded: %s" % os.path.split(file)[1])
	except Exception as e:
		print("POST analyze failed:\n%s" % str(e))
		quit()

########### Get analyze results section #############

# 複数ファイル対応
array_resp_json_dict = {} # {filename, 抽出結果json(analyzeResult以下)}

n_tries = 15
n_try = 0
wait_sec = 5
max_wait_sec = 60

for get_url_dict in getUrls_dict.items():
	while n_try < n_tries:
	    try:
	        resp = get(url = get_url_dict[1], headers = {"Ocp-Apim-Subscription-Key": apim_key})
	        resp_json = resp.json()
	        if resp.status_code != 200:
	            print("GET analyze results failed:\n%s" % json.dumps(resp_json))
	            quit()
	        status = resp_json["status"]
	        if status == "succeeded":
	            array_resp_json_dict[get_url_dict[0]] = resp_json['analyzeResult']['documentResults'][0]
	            print("Analysis succeeded:%s" % get_url_dict[0])
	            break
	        if status == "failed":
	            print("Analysis failed:\n%s" % json.dumps(resp_json))
	            quit()
	        # Analysis still running. Wait and retry.
	        time.sleep(wait_sec)
	        n_try += 1
	        wait_sec = min(2*wait_sec, max_wait_sec)     
	    except Exception as e:
	        msg = "GET analyze results failed:\n%s" % str(e)
	        print(msg)
	        quit()

########### 抽出結果のcsv出力 section #############

## Model APIからTag一覧を取得

model_url = endpoint + "formrecognizer/v2.0/custom/models/" + model_id

modelresp = get(url = model_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
model_resp_json = modelresp.json()

# csvヘッダ行作成
taglist = [] # タグ一覧の配列
taglist.append("_filename")
for myvalue in model_resp_json['trainResult']['fields']:
  taglist.append(myvalue['fieldName'])

# csvデータ行作成
rowdictlist = []
for resp_json_dict in array_resp_json_dict.items(): # resp_json_dictは{filename, 抽出結果json(analyzeResult以下)}の1行
	resp_json_fields = resp_json_dict[1]['fields'] #抽出結果jsonのdocumentResults/fields以下を取得

	rowdict = {} # 1データ行を示すdict
	for myvalue in taglist:
		rowdict[myvalue] = '' #rowdict初期化
	rowdict["_filename"] = resp_json_dict[0]

	for mykey in resp_json_fields.keys(): # fields以下のentityを1つずつ処理
		val = resp_json_fields[mykey]

		# 低confidence対応
		try:
			if val['confidence'] > confidence_setting:
				rowdict[mykey] = val['text'].replace(" ","") # 空白除去対応
			else:
				rowdict[mykey] = '['+str(val['confidence'])+']'
		except Exception as e:
			msg = "PARSE error:%s" % str(e)
			print(msg)

	rowdictlist.append(rowdict) #rowdictをlistに積む

# print("Row dict list:\n%s" % rowdictlist)

#csvに出力
now = datetime.datetime.now()
filename = now.strftime('%Y%m%d_%H%M%S') + '.csv'
with open(filename, 'w', newline="") as f:
    writer = csv.DictWriter(f, taglist)
    writer.writeheader()
    for row in rowdictlist:
      writer.writerow(row)

print("Analyze done.  Output CSV fine: %s" % str(filename))