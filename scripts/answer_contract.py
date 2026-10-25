"""Reference-independent answer formatting for extractive QA.

Examples are synthetic, unrelated to evaluation answers. Parsing never sees
gold answers, trims prefixes, searches alternatives or repairs malformed JSON.
The model's raw response remains the metered, archived platform result.
"""
import json

CONTRACTS = ('span-v1', 'json-span-v1')
SYSTEM = 'Answer the question using the passage. Return only the shortest answer span from the passage. Do not explain or add a prefix.'


def messages(context, question, contract='span-v1'):
    user = {'role': 'user', 'content': 'Passage:\n' + context + '\n\nQuestion: ' + question}
    if contract == 'span-v1':
        return [{'role': 'system', 'content': SYSTEM}, user]
    if contract != 'json-span-v1':
        raise ValueError('Unknown answer contract')
    return [
        {'role': 'system', 'content': 'Extract the shortest passage span that answers the question. Copy its exact characters. Return one JSON object with exactly one string field "answer". No explanation, Markdown or extra fields.'},
        {'role': 'user', 'content': 'Passage:\nMira left the blue notebook beside the north gate on Tuesday.\n\nQuestion: Where did Mira leave the notebook?'},
        {'role': 'assistant', 'content': '{"answer":"north gate"}'},
        {'role': 'user', 'content': 'Passage:\nThe Leto observatory opened in 1987 and closed in 2012.\n\nQuestion: In which year did the observatory open?'},
        {'role': 'assistant', 'content': '{"answer":"1987"}'},
        user,
    ]


def parse_answer(raw, context, contract='span-v1'):
    if not isinstance(raw, str):
        raise ValueError('Final text required')
    if contract == 'span-v1':
        return raw
    if contract != 'json-span-v1':
        raise ValueError('Unknown answer contract')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=unique)
    if (type(value) is not dict or set(value) != {'answer'} or
            type(value['answer']) is not str or not 0 < len(value['answer']) <= 4096 or
            value['answer'] not in context):
        raise ValueError('One nonempty literal passage span required')
    return value['answer']
