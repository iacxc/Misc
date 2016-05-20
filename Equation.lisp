
(defun det2 (a1 a2 b1 b2)
    (- (* a1 b2) (* a2 b1)))

(defun answer (a1 a2 b1 b2 c1 c2)
    (let ((x (/ (det2 c1 c2 b1 b2) (det2 a1 a2 b1 b2)))
          (y (/ (det2 a1 a2 c1 c2) (det2 a1 a2 b1 b2))))
         (format t "x = ~A, y = ~A~%" x y)
         (format t "~A * ~A + ~A * ~A = ~A~%" a1 x b1 y (+ (* a1 x) (* b1 y)))
         (format t "~A * ~A + ~A * ~A = ~A~%~%" a2 x b2 y (+ (* a2 x) (* b2 y)))))

(answer 2 5 -3 -4 -11 -3)
(answer 2 5  3 -6   1  7)
(answer 6 5 -3 -9  -3  4)
(answer 7 3  4 -6   2 24)
