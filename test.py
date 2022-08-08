import requests
import sys

"""
Use remote image object detection service provided by Andrew
"""
page = 'http://localhost:8085/upload'

# 35.88.99.235
# 8080
# ascott
# 2TAyme29FjNzpHKE
# J6FLgLawuqzbEHsGzxm35GumUNGk4gbZAQ2WrcWdet4zLDFFesMERbgT4LqHwrGK


multipart_form_data = {
    'threshold': ('', str(0.1)),
    'img_file': sys.argv[1],
}
print(multipart_form_data)
try:
    response = requests.post(page, files=multipart_form_data)
    if response.status_code != 200:
        print("Server returned status {}.".format(response.status_code))

    print(response.text)
except:
    response = requests.post(page, files=multipart_form_data)
    if response.status_code != 200:
        print("Server returned status {}.".format(response.status_code))

    print(response.text)
