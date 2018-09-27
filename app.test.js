import app from './app'
import 'mocha'
import chai from 'chai'

chai.should();

describe('app', function() {
    it('should return true', function() {
        app().should.be.true
    });
});