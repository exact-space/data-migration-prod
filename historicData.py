from dataMigrationImpl import dataMiragtion,os,requests,json,createConfig

# unitsId = os.environ.get("unitsId")
# if unitsId == None:
    # print("Pass in valid unitsId \n exiting...")
    # exit()
    

# lst = ["61c0c34bb45a623b64fc3b12","630c916f8c19444ca8aed6fb","62c6af10c8e88f754735088b","62ff525f0053c325ccf27a1d"
# ,"6304607bc3a07014f78f49ad"]

# lst = ["61c0c2fdb45a623b64fc3b0f","61c0c34bb45a623b64fc3b12","61c1818371c20d4a206a2e35","61c4b560515e2f6d59c00202",
# "62c6af10c8e88f754735088b","62ff525f0053c325ccf27a1d","630c916f8c19444ca8aed6fb","637484d11a7c8a00087cf4c8",
# "638dc62f6049ba000768c7d2","639810021dda6e0007878d47","63b5812dac390b000789bf2b","640986a092fcf10007c2b6cd",
# "6492c02716dc050007728faa","64ca3ca0661bc30007a5c613"]

# for i in lst[1:2]:

#     dm = dataMiragtion(i)
#     dm.prodFunc()
#     # dm.mainFunc(False)
#     # dm.mainFuncReport()
# exit()
      
# url = "https://data.exactspace.co/exactapi/units"
# # url = "https://data.exactspace.co/exactapi/units?filter=%7B%22where%22%3A%7B%22bu%22%3A%22heating%22%7D%7D"
# res = requests.get(url)
# body = json.loads(res.content)
# lst = []
# for i in body:
#     lst.append(i["id"])

# url = "https://data.exactspace.co/exactapi/units"
# res = requests.get(url)
# body = json.loads(res.content)
# for i in body:
    # if i["id"] not in lst:
        # dm = dataMiragtion(i["id"])
        # dm.mainFunc(False)
    # dm.mainFuncReport()
# exit()

# for i in range(1,2):
#     createConfig("remUnits"+str(i)+".json",body,"id")
url = "https://data.exactspace.co/exactapi/units"
res = requests.get(url)
body = json.loads(res.content)

body = [{"id":"628dd242c78e4c5d0f3b90cf"},{"id":"635219343e4a8c0006f29888"}]
fileName = "sandbox2.json"
print(fileName)
# createConfig(fileName,body,"id")

for j in body:
    i = j["id"]
    file_path = fileName
    rightWord = "Done"

    
    # Open the file in read mode and load the JSON data
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
    
    if data[i]:
        count = data[i]
    else:
        count = 0

    if data[i] != rightWord:
        dm = dataMiragtion(i)
        dm.mainFuncDataMigration(count,file_path=fileName,deleteData=False,monthNum=1,onlyRaw = True)
        # dm.mainFuncReport()
    else:
        print(i, "already done...")
        
    # Open the file in read mode and load the JSON data
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
        
    data[i] = rightWord
    # Open the file in write mode and save the data as JSON
    with open(file_path, "w") as json_file:
        json.dump(data, json_file)
