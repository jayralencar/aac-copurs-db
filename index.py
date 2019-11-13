import sqlite3
import spacy

nlp = spacy.load("en_core_web_sm")


conn = sqlite3.connect('./db/acctext.db')


files = [
    {
        "path":"./aac_comm/sent_dev_aac.txt",
        "type":"dev"
    },
    {
        "path":"./aac_comm/sent_test_aac.txt",
        "type":"test"
    },
    {
        "path":"./aac_comm/sent_train_aac.txt",
        "type":"train"
    },
]

def create_file(path,type):
    c = conn.cursor()
    for row in c.execute("SELECT * FROM files WHERE name = '"+path+"' and type = '"+type+"'"):
        return row[0]
    c.execute("INSERT INTO files (name,type) VALUES (?,?)",(path,type))
    return c.lastrowid

def create_utterance(gloss, file_id, utterance_sequence):
    c = conn.cursor()
    for row in c.execute("SELECT * FROM utterance WHERE file_id = '"+str(file_id)+"'  and utterance_sequence = '"+str(utterance_sequence)+"'"):
        return row[0]
    c.execute("INSERT INTO utterance (gloss,file_id,utterance_sequence ) VALUES (?,?,?)",(gloss,file_id, utterance_sequence))
    return c.lastrowid

def create_token(token):
    c = conn.cursor()
    for row in c.execute("SELECT * FROM token WHERE utterance_id = '"+str(token['utterance_id'])+"'  and token_sequence = '"+str(token['token_sequence'])+"'"):
        return row[0]
    c.execute("INSERT INTO token (gloss,utterance_id,lemma,pos,dep, head,token_sequence ) VALUES (?,?,?,?,?,?,?)",(token["gloss"],token["utterance_id"],token['lemma'],token['pos'],token['dep'],token['head'],token['token_sequence']))
    return c.lastrowid

for file in files:
    file_id = create_file(file['path'],file['type'])

    f = open(file['path'])
    i = 0
    for line in f:
        sentence = line.rstrip().decode('utf8')
        utterance_id = create_utterance(sentence,file_id,i)
        doc = nlp(sentence)
        for token in doc:
            token_id = create_token({
                "gloss": token.text,
                "utterance_id":utterance_id,
                "lemma":token.lemma_,
                "pos":token.pos_,
                "dep":token.dep_,
                "head":token.head.i,
                "token_sequence": token.i
            }) 
            print(token,token.i, token.lemma_, token.pos_, token.dep_, token.head.i)
        i = i + 1

    # print(len(f.readlines()))


conn.commit()
conn.close()