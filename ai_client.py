import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))


def generate(prompt: str, model: str = "llama-3.1-8b-instant") -> str:
    """
    Makes a Groq API call.
    14,400 free requests per day — no quota issues.
    """
    import time

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=2000
            )
            return response.choices[0].message.content

        except Exception as e:
            err = str(e)
            if "429" in err and attempt < 2:
                print(f"[Groq] Rate limited, retrying in 5s (attempt {attempt + 1}/3)...")
                time.sleep(5)
            else:
                raise e

    raise Exception("[Groq] Failed after 3 attempts")