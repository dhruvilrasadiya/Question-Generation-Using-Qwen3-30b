from confluent_kafka import Consumer, Producer
from langchain_community.llms import Ollama
from env import DATABASE_URL
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from sqlalchemy import create_engine, Column, Integer, String, VARCHAR, UUID, text, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from bs4 import BeautifulSoup
from pydantic import BaseModel
import json
import time
import random
import re

# Create a PostgreSQL engine
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

class KafkaRequest(BaseModel):
    category_id: str
    message_id: str
    instruction: str    
    complexity: str
    blooms: str
    output_format: str
    tenant: str

class Option(BaseModel):
    optionId: str
    optionText: str

class KafkaResponse(BaseModel):
    item: str
    options: list[Option]
    correct_answer: str
    message_id: str

# Define the knowledge table
class knowledge(Base):
    __tablename__ = 'knowledge'
    id = Column(Integer, primary_key=True)
    category_id = Column(VARCHAR(255))
    text = Column(VARCHAR)
    chunk_id = Column(UUID(as_uuid=True))
    file_id = Column(VARCHAR(255))
    

llm = Ollama(model="qwen3:30b")
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
    # 'enable.auto.commit': 'false'
}

consumer = Consumer(conf)
consumer.subscribe(['panini-item-request'])


# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)


def custom_deserializer(value):
    if value:
        try:
            return json.loads(value.decode('utf-8'))
        except json.JSONDecodeError:
            return value.decode('utf-8')  # Fallback to plain text
    return None


def send_response(topic: str, response: dict):
    # Create a KafkaResponse object from the response_data dictionary
    # response = KafkaResponse(**response)
    # Send the response back to the Kafka topic
    producer.produce(topic, value=json.dumps(response))
    producer.flush()
    print("Sent response!")


def get_documents_from_db(category_id):
    session = Session()
    total_records = session.query(knowledge).filter(knowledge.category_id == category_id).count()
    print(total_records)
    limit = 5
    max_starting_point = max(0, total_records - limit)  # Ensure we don't go out of bounds
    random_starting_point = random.randint(0, max_starting_point)
    knowledge_text = (
        session.query(knowledge)
        .filter(knowledge.category_id == category_id)
        .order_by(func.random())  # Randomize the order of the records
        .offset(random_starting_point)  # Use the random starting point
        .limit(limit)  # Limit to 200 records
        .all()
    )
    # formatted_knowledge_text = [chunk.text.replace('\\n', '\n') for chunk in knowledge_text]
    final_text = []
    for chunk in knowledge_text:
        chunk_text = chunk.text
        final_text.append(chunk_text)
    # Join the list into a single string with newlines separating each chunk
    final_text = "\n".join(final_text)
    # chunks_in_knowledge_text = [chunk.text for chunk in knowledge_text]
    print(final_text)
    return final_text 


def format_docs(docs):
    # Example of simple concatenation; adjust based on your requirements
    formatted = "\n".join(docs)  # Assuming docs are strings
    return formatted


def generate_mcq_chain(category_id, user_prompt,rag_prompt):
    # Step 1: Retrieve relevant documents from the database
    docs = get_documents_from_db(category_id)
    if not docs:
        raise ValueError("No relevant documents found in the database for the given category ID")
    # Step 3: Set up the chain with the formatted context
    chain = (
        RunnablePassthrough.assign(context=lambda input: docs)  # Assign the context
        | rag_prompt  # Pass to the prompt
        | llm # Run through the model to generate the MCQ
        | StrOutputParser()  # Parse the output if necessary
    )
    # Step 4: Invoke the chain
    result = chain.invoke({"context": docs, "user_prompt": user_prompt})
    return result, docs


def main():
    session = Session()
    try:
        while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                try:
                    message_value = custom_deserializer(msg.value())
                    if message_value is None:
                        print("Received empty message")
                        continue
                    # Validate message structure using Pydantic
                    request_data = KafkaRequest(**message_value)
                    print("Request data:")
                    print(request_data)
                    print("======")
                    category_id = request_data.category_id
                    message_id = request_data.message_id
                    tenant = request_data.tenant
                    instruction = request_data.instruction
                    if not instruction or instruction == "NA":
                        instruction = """You are an expert multiple-choice item writer. You are to write 4 options, single response, multiple choice test items. Make sure to phrase the item considering that the reader will not be able to see the context or passage that you are seeing. Don't use "according to the passage","as stated in the passage" in the item stem.The item stem and responce options for each item can have upto 200 words. Each of the answer options should be of comparable structure and length. Distractors for math items should be derived from incorrectly calculating the information. Create plausible, specific and realistic distractor options but make them clearly incorrect. Include common error or misconceptions. Avoid absurd, ridiculous, humorous or tricky options. For example , don’t include response options that calls to do things outside of their role or expertise. Don’t use “All of the above” and “None of the above”. Don’t combine response options. The response options should not overlap, they should be mutually exclusive. Your output should include questions that focus on a given specific cognitive level of Bloom's Taxonomy. The output format must be strictly maintained as given. Do not generate anything outside the output format json."""
                    print("Instruction:")
                    print(instruction)
                    print("======")
                    complexity = request_data.complexity
                    if not complexity or complexity == "NA":
                        complexity = """generate only one item on the below cognitive level instruction, specifically targeting easy complexity. Make sure to maintain the OUTPUT FORMAT of the question as given below. Do not add any additional explanation in the output. Strictly follow the OUTPUT FORMAT for generating the question and provide the correct answer as mentioned in the output format. if the response options is already mentioned in the output format then do not generate any other response options. use that options and make item stem accordingly.only generate those things which is mentioned in squared brackets in the output format.Your output should include questions that focus on a given specific cognitive level of Bloom's Taxonomy.
                        """
                    print("Complexity:")
                    print(complexity)
                    print("======")
                    blooms = request_data.blooms
                    if not blooms or blooms == "NA":
                        blooms = """Focus on recalling factual information, definitions, or basic concepts. Ensure that the questions require the test-taker to remember and recognize information rather than apply or analyze it. Use straightforward language and clear prompts that guide the test-taker to retrieve specific knowledge."""
                    print("Blooms:")
                    print(blooms)
                    print("======")
                    output_format = request_data.output_format
                    if not output_format or output_format == "NA":
                        output_format="""REPLY ONLY IN THE FOLLOWING JSON OUTPUT FORMAT
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
                                "correct_answer": "Specify the correct answer."
                            }
                        """
                    extention = """<context>
                        {context}
                        </context>
                        follow the below instruction:
                        {user_prompt}
                        """
                    user_prompt = complexity + blooms + output_format
                    system_prompt = instruction + extention
                    rag_prompt = ChatPromptTemplate.from_template(system_prompt)
                    item, docs = generate_mcq_chain(category_id, user_prompt, rag_prompt)
                    json_part = re.sub(r"<think>.*?</think>", "", item, flags=re.DOTALL)
                    print(item)
                    item_json = json.loads(json_part.strip())
                    item_json['message_id'] = message_id
                    item_json['tenant'] = tenant
                    item_json['reference_text'] = docs
                    send_response('panini-item-response', item_json)
                    consumer.commit()
                except Exception as e:
                    # send_response('panini-item-response',  {str(e)})
                    print(f"Error processing message: {str(e)}")
                    continue

    except KeyboardInterrupt:
        print("Shutting down consumer...")

    finally:
        consumer.close()

        
if __name__ == "__main__":
    main()