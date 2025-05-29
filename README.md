# Movie Data Fetcher

This script fetches movie data from the TMDB API and parses it into a workable data structure.

## Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Create a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Create a .env File
Create a `.env` file in the root directory with the following content:
```
API_KEY=your_tmdb_api_key_here
```

### 5. Generate requirements.txt
If you add new dependencies, update `requirements.txt` using:
```bash
pip freeze > requirements.txt
```

### 6. Using .gitignore
Ensure your `.gitignore` file includes:
```
venv/
.env
__pycache__/
*.pyc
```

## Usage
Run the script:
```bash
python getMovies.py
```

This will fetch all available movies and print the top 5 rated movies.

## Git Usage

### Branches
- **Create a new branch:**
  ```bash
  git checkout -b <branch-name>
  ```
- **Switch to an existing branch:**
  ```bash
  git checkout <branch-name>
  ```
- **List all branches:**
  ```bash
  git branch
  ```

### Stashing
- **Stash changes:**
  ```bash
  git stash
  ```
- **Apply stashed changes:**
  ```bash
  git stash apply
  ```
- **List stashes:**
  ```bash
  git stash list
  ```

### Commits
- **Stage changes:**
  ```bash
  git add <file>
  ```
- **Commit changes:**
  ```bash
  git commit -m "Your commit message"
  ```
- **Push changes to remote:**
  ```bash
  git push origin <branch-name>
  ```

### Good Practices
- **Commit Messages:** Write clear, descriptive commit messages.
- **Branch Naming:** Use meaningful branch names (e.g., `feature/add-login`, `bugfix/fix-crash`).
- **Pull Before Push:** Always pull the latest changes before pushing your work.
- **Code Review:** Review your changes before committing.
- **Avoid Committing Sensitive Data:** Never commit API keys, passwords, or other sensitive information.