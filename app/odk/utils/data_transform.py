import xmltodict, json

def xml_to_json(xml_data):
    """
     Parses the XML data and returns json representation of the same data

     :params xml_data: string of XML data
     :return: json representation of the xml_data
    """
    xml_dict = xmltodict.parse(xml_data)

    
    json_data = json.dumps(xml_dict, indent=4)
    return json.loads(json_data)


# Function to convert an Element object to a dictionary
def element_to_dict(element):
    result = {}
    for child in element:
        child_dict = element_to_dict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = [result[child.tag], child_dict]
        else:
            result[child.tag] = child_dict
    return result


def flattenTranslations(questions_texts, key = 'text'):
    """
        Flatterns the array inside an array to get a flat array from ODK questions
    """
    flat_list = [question for questions in questions_texts for question in questions[key]]
    return flat_list

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def flatten_preserve_array_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                if isinstance(a, dict):
                    for k, v in flatten_preserve_array_json(a).items():
                        out[name + str(i) + '_' + k] = v
                else:
                    out[name + str(i)] = a
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def odk_questions_formatter(odk_questions):
    """
        Formats ODK questionnare data.
        :param odk_questions: Array of questions in json format
        :returns Filterred and formatted questions in json
    """
    id_splits = {}

    # Filter the questions and process each one
    questions = filter(
        lambda question: not any(
            substr in question.get('@id', '')
            for substr in ['hint', 'Hint', 'constraintMsg']
        ),
        odk_questions
    )

    # new_questions = []

    for question in questions:
        id_values = question.get('@id', '')
        if id_values:
            question_id = id_values.split(":")[0].split("-")
            question_id = "_".join(question_id)
            id_splits[question_id] = question.get('value')

    return id_splits


def filter_non_questions(fields):
    """
     Filters ODK non questions in questions list of the form.
     :param fields: Array of fields
     :returns Filterred and formatted fields in json array
    """
    questions = list(filter(
        lambda field: 'note' not in field.get('type', '') and 
                      field.get('type') != 'structure' and 
                      field.get('type') != 'repeat',
        fields
    ))
    return questions


def assign_questions_options(field, questions):
    """
        Assign options to the optionated questions from ODK formatted questionnare
        :param field: ODK question field {path: label}
        :param questions: Dictionary of questions 
        {
            "path": "/sample/path",
            "name": "path",
            "type": "string",
            "binary": null,
            "selectMultiple": null
        }

        :return questions with options
    """
    
    options = [
        {
            "path": option ,
            "value": option.split('/')[len(option.split('/')) - 1],
            "label": questions[option]
        }
        for option in questions.keys()
        if field['path'] in option
        and not option.endswith(field['path'])
        and len(option.split(field['path'])[1].split("/")) == 2 and option.split(field['path'])[1].split("/")[0] == ""
    ]
    label = [
        questions[key]
        for key in questions.keys() 
        if key.endswith(field['path'])
    ]
    label = label[0] if len(label) > 0 else ""
    if type(label) != str and type(label) == dict:
        label = label.get('#text', "")
    return {
        **field,
        "path": field['path'].lower(),
        "name": field['name'].lower(),
        "label": label,
        "options": options
    }