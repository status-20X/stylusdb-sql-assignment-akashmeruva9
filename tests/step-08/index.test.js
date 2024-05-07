// tests/index.test.js

const executeSELECTQuery = require('../../src/index');
const parseQuery = require('../../src/queryParser');

test('Parse SQL Query with INNER JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        whereClauses: [],
        joinTable: 'enrollment',
        joinCondition: {
            left: 'student.id',
            right: 'enrollment.student_id'
        }
    });
});

test('Parse SQL Query with INNER JOIN and WHERE Clause', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.age > 20';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        whereClauses: [{ field: 'student.age', operator: '>', value: '20' }],
        joinTable: 'enrollment',
        joinCondition: {
            left: 'student.id',
            right: 'enrollment.student_id'
        }
    });
});

test('Execute SQL Query with INNER JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBe(4);
    expect(result[0]).toEqual({ 'student.name': 'John', 'enrollment.course': 'Mathematics' });
    expect(result[1]).toEqual({ 'student.name': 'John', 'enrollment.course': 'Physics' });
    expect(result[2]).toEqual({ 'student.name': 'Jane', 'enrollment.course': 'Chemistry' });
    expect(result[3]).toEqual({ 'student.name': 'Bob', 'enrollment.course': 'Mathematics' });
});

test('Execute SQL Query with INNER JOIN and a WHERE Clause', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.age > 25';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBe(3);
    expect(result[0]).toEqual({ 'student.name': 'John', 'enrollment.course': 'Physics' });
    expect(result[1]).toEqual({ 'student.name': 'Jane', 'enrollment.course': 'Chemistry' });
    expect(result[2]).toEqual({ 'student.name': 'Bob', 'enrollment.course': 'Mathematics' });
});
