const questions = [
  {
    q: "What is HotStuff mainly used for?",
    options: [
      "NFT minting",
      "Blockchain consensus",
      "Crypto trading",
      "Wallet security"
    ],
    answer: 1
  },
  {
    q: "Which problem does HotStuff improve over older BFT protocols?",
    options: [
      "High gas fees",
      "Slow finality and communication",
      "Token inflation",
      "Energy consumption"
    ],
    answer: 1
  },
  {
    q: "HotStuff was originally designed for which project?",
    options: [
      "Ethereum",
      "Bitcoin",
      "Diem (Libra)",
      "Solana"
    ],
    answer: 2
  },
  {
    q: "What makes HotStuff scalable?",
    options: [
      "Proof of Work",
      "Quadratic messaging",
      "Linear communication",
      "Central authority"
    ],
    answer: 2
  },
  {
    q: "HotStuff belongs to which family of protocols?",
    options: [
      "PoS",
      "BFT",
      "PoW",
      "DAG"
    ],
    answer: 1
  }
];

let current = 0;
let score = 0;
let timerInterval = null;
let timeLeft = 30;

const startScreen = document.getElementById("start-screen");
const quizScreen = document.getElementById("quiz-screen");
const resultScreen = document.getElementById("result-screen");

const questionEl = document.getElementById("question");
const optionsEl = document.getElementById("options");
const progressEl = document.getElementById("progress");
const scoreText = document.getElementById("score-text");
const timerEl = document.getElementById("timer");

document.getElementById("start-btn").onclick = () => {
  startScreen.classList.remove("active");
  quizScreen.classList.add("active");
  loadQuestion();
};

function loadQuestion() {
  const q = questions[current];
  questionEl.textContent = q.q;
  progressEl.textContent = `Question ${current + 1}/${questions.length}`;
  optionsEl.innerHTML = "";

  q.options.forEach((opt, i) => {
    const div = document.createElement("div");
    div.className = "option";
    div.innerHTML = `
      <div class="option-letter">${String.fromCharCode(65 + i)}</div>
      <div>${opt}</div>
    `;
    div.onclick = () => selectOption(div, i);
    optionsEl.appendChild(div);
  });

  startTimer();
}

function startTimer() {
  clearInterval(timerInterval);
  timeLeft = 30;
  timerEl.textContent = timeLeft + "s";

  timerInterval = setInterval(() => {
    timeLeft--;
    timerEl.textContent = timeLeft + "s";

    if (timeLeft <= 0) {
      clearInterval(timerInterval);
      autoNextQuestion();
    }
  }, 1000);
}

function selectOption(el, index) {
  clearInterval(timerInterval);

  const correct = questions[current].answer;
  const options = document.querySelectorAll(".option");

  options.forEach((opt, i) => {
    opt.onclick = null;
    if (i === correct) opt.classList.add("correct");
    if (i === index && i !== correct) opt.classList.add("wrong");
  });

  if (index === correct) score++;

  setTimeout(() => {
    current++;
    if (current < questions.length) {
      loadQuestion();
    } else {
      endQuiz();
    }
  }, 1200);
}

function autoNextQuestion() {
  current++;
  if (current < questions.length) {
    loadQuestion();
  } else {
    endQuiz();
  }
}

function endQuiz() {
  quizScreen.classList.remove("active");
  resultScreen.classList.add("active");
  scoreText.textContent = `You scored ${score} out of ${questions.length}`;
}
