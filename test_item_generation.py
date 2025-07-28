from confluent_kafka import Producer
import json


# Create a Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}


producer = Producer(producer_conf)


# Function to send a one-statement query
def send_query(topic, category_id, message_id, instruction, complexity, blooms, output_format, tenant):
    message = {'category_id': category_id, 'message_id': message_id, 'instruction': instruction, 'complexity': complexity, 'blooms': blooms, 'output_format': output_format, 'tenant': tenant}
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print(f'Sent: {message}')


# Specify your topic name
topic_name = 'panini-item-request'


# Example query to send
category_id = "95b39057-ffb2-49bb-6ddf-08dd589efe9a"

message_id = "97e26372-b8c8-4cd9-a58c-08dd57efa9bd"

instruction = """You are an expert multiple-choice item writer. You are to write 4 options, single response, multiple choice test items.The item stem and responce options for each item can have upto 200 words. Each of the answer options should be of comparable structure and length. Distractors for math items should be derived from incorrectly calculating the information. The stem of each item should either end with question mark, or an incomplate sentence that is complated by any of the response options. Create a positive item stem, avoiding words like “EXCEPT” and “NOT”. Create plausible, specific and realistic distractor options but make them clearly incorrect. Include common error or misconceptions. Avoid absurd, ridiculous, humorous or tricky options. For example , don’t include response options that calls to do things outside of their role or expertise. Don’t use “All of the above” and “None of the above”. Don’t combine response options. The response options should not overlap, they should be mutually exclusive. Your output should include questions that focus on a given specific cognitive level of Bloom's Taxonomy. The output format must be strictly maintained as given. Do not generate anything outside the output format json.
"""
complexity = """generate only one item on the below cognitive level instruction, specifically targeting easy complexity. Make sure to maintain the OUTPUT FORMAT of the question as given below. Do not add any additional explanation in the output. Strictly follow the OUTPUT FORMAT for generating the question and provide the correct answer as mentioned in the output format. if the response options is already mentioned in the output format then do not generate any other response options. use that options and make item stem accordingly.only generate those things which is mentioned in squared brackets in the output format.Your output should include questions that focus on a given specific cognitive level of Bloom's Taxonomy.
"""
blooms = """Focus on recalling factual information, definitions, or basic concepts. Ensure that the questions require the test-taker to remember and recognize information rather than apply or analyze it. Use straightforward language and clear prompts that guide the test-taker to retrieve specific knowledge.
"""
output_format = """
OUTPUT FORMAT
{
    "item": "Insert the question stem here",
    "options": [
        {
            "optionId": "1",
            "optionText": "Insert Option 1 here"
        },
        {
            "optionId": "2",  
            "optionText": "Insert Option 2 here" 
        },
        {
            "optionId": "3", 
            "optionText": "Insert Option 3 here"  
        },
        {
            "optionId": "4",  
            "optionText": "Insert Option 4 here" 
        }
    ],
    "correct_answer": "Specify the correct answer's optionId."
}
"""

tenant = "1111"
# Send the one-statement query
send_query(topic_name, category_id, message_id, instruction, complexity, blooms, output_format, tenant)