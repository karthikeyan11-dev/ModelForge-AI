import React from 'react';
import { motion } from 'framer-motion';

const ML_TERMS = [
  "Random Forest", "XGBoost", "Logistic Regression", 
  "SVM", "Neural Network", "Gradient Boosting",
  "Feature Engineering", "AutoML", "Data Cleaning"
];

const AnimatedBackground = () => {
  return (
    <div className="fixed inset-0 pointer-events-none -z-20 overflow-hidden bg-[radial-gradient(circle_at_top_right,#f3e8ff,white_50%)]">
      {/* Dynamic Floating Terms */}
      {ML_TERMS.map((term, i) => (
        <motion.div
          key={i}
          initial={{ 
            opacity: 0, 
            x: Math.random() * 100 + "%", 
            y: Math.random() * 100 + "%" 
          }}
          animate={{ 
            opacity: [0.03, 0.08, 0.03],
            y: ["0%", "-10%", "0%"],
            x: ["0%", "2%", "0%"],
          }}
          transition={{
            duration: 15 + Math.random() * 20,
            repeat: Infinity,
            ease: "easeInOut",
            delay: i * 2,
          }}
          className="absolute text-purple-900/10 font-black tracking-tighter whitespace-nowrap select-none blur-[1px]"
          style={{
            fontSize: `${2 + Math.random() * 4}rem`,
            left: `${Math.random() * 80}%`,
            top: `${Math.random() * 80}%`,
          }}
        >
          {term}
        </motion.div>
      ))}

      {/* Abstract Gradient Orbs */}
      <div className="absolute top-[-10%] right-[-10%] w-[50%] h-[50%] bg-purple-300/20 blur-[120px] rounded-full animate-float" />
      <div className="absolute bottom-[-10%] left-[-10%] w-[40%] h-[40%] bg-indigo-300/10 blur-[100px] rounded-full animate-float [animation-delay:2s]" />
    </div>
  );
};

export default AnimatedBackground;
