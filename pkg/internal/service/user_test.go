package service_test

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/pyroscope-io/pyroscope/pkg/internal/model"
	"github.com/pyroscope-io/pyroscope/pkg/internal/service"
)

var _ = Describe("UserService", func() {
	s := new(testSuite)
	BeforeEach(s.BeforeEach)
	AfterEach(s.AfterEach)

	var svc service.UserService
	BeforeEach(func() {
		svc = service.NewUserService(s.DB())
	})

	Describe("user creation", func() {
		var (
			params = testCreateUserParams()[0]
			user   *model.User
			err    error
		)

		JustBeforeEach(func() {
			user, err = svc.CreateUser(context.Background(), params)
		})

		Context("when a new user created", func() {
			It("does not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("should populate the fields correctly", func() {
				expectUserMatches(user, params)
			})

			It("creates valid password hash", func() {
				err = model.VerifyPassword(user.PasswordHash, params.Password)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when user email is already in use", func() {
			BeforeEach(func() {
				_, err = svc.CreateUser(context.Background(), params)
				Expect(err).ToNot(HaveOccurred())
			})

			It("returns validation error", func() {
				Expect(model.IsValidationError(err)).To(BeTrue())
			})
		})

		Context("when user is invalid", func() {
			BeforeEach(func() {
				params = model.CreateUserParams{}
			})

			It("returns validation error", func() {
				Expect(model.IsValidationError(err)).To(BeTrue())
			})
		})
	})

	Describe("user retrieval", func() {
		var (
			params = testCreateUserParams()[0]
			user   *model.User
			err    error
		)

		Context("when an existing user is queried", func() {
			BeforeEach(func() {
				user, err = svc.CreateUser(context.Background(), params)
				Expect(err).ToNot(HaveOccurred())
			})

			It("can be found", func() {
				By("id", func() {
					user, err = svc.FindUserByID(context.Background(), user.ID)
					Expect(err).ToNot(HaveOccurred())
					expectUserMatches(user, params)
				})

				By("email", func() {
					user, err = svc.FindUserByEmail(context.Background(), params.Email)
					Expect(err).ToNot(HaveOccurred())
					expectUserMatches(user, params)
				})
			})
		})

		Context("when a non-existing user is queried", func() {
			It("returns ErrUserNotFound error of NotFoundError type", func() {
				By("id", func() {
					_, err = svc.FindUserByID(context.Background(), 0)
					Expect(err).To(MatchError(model.ErrUserNotFound))
					Expect(model.IsNotFoundError(err)).To(BeTrue())
				})

				By("email", func() {
					_, err = svc.FindUserByEmail(context.Background(), params.Email)
					Expect(err).To(MatchError(model.ErrUserNotFound))
					Expect(model.IsNotFoundError(err)).To(BeTrue())
				})
			})
		})
	})

	Describe("users retrieval", func() {
		var (
			params = testCreateUserParams()
			users  []*model.User
			err    error
		)

		JustBeforeEach(func() {
			users, err = svc.GetAllUsers(context.Background())
		})

		Context("when all users are queried", func() {
			BeforeEach(func() {
				for _, user := range params {
					_, err = svc.CreateUser(context.Background(), user)
					Expect(err).ToNot(HaveOccurred())
				}
			})
			It("returns all users", func() {
				Expect(err).ToNot(HaveOccurred())
				user1, err := svc.FindUserByEmail(context.Background(), params[0].Email)
				Expect(err).ToNot(HaveOccurred())
				user2, err := svc.FindUserByEmail(context.Background(), params[1].Email)
				Expect(err).ToNot(HaveOccurred())
				Expect(users).To(ConsistOf(user1, user2))
			})
		})

		Context("when no users exist", func() {
			It("returns no error", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(users).To(BeEmpty())
			})
		})
	})

	Describe("user update", func() {
		var (
			params  = testCreateUserParams()
			update  model.UpdateUserParams
			user    *model.User
			updated *model.User
			err     error
		)

		JustBeforeEach(func() {
			updated, err = svc.UpdateUserByID(context.Background(), user.ID, update)
		})

		Context("when no parameters specified", func() {
			BeforeEach(func() {
				user, err = svc.CreateUser(context.Background(), params[0])
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not modify user", func() {
				Expect(err).ToNot(HaveOccurred())
				expectUserMatches(updated, params[0])
			})
		})

		Context("when parameters provided", func() {
			BeforeEach(func() {
				user, err = svc.CreateUser(context.Background(), params[0])
				Expect(err).ToNot(HaveOccurred())
				adminRole := model.AdminRole
				update = model.UpdateUserParams{
					FullName: model.String("Jonny"),
					Email:    model.String("admin@local.domain"),
					Role:     &adminRole,
				}
			})

			It("updates user fields", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(updated.FullName).To(Equal(*update.FullName))
				Expect(updated.Email).To(Equal(*update.Email))
				Expect(updated.Role).To(Equal(*update.Role))
				Expect(updated.CreatedAt).ToNot(BeZero())
				Expect(updated.UpdatedAt).ToNot(BeZero())
				Expect(updated.UpdatedAt).ToNot(Equal(updated.CreatedAt))
			})
		})

		Context("when parameters invalid", func() {
			BeforeEach(func() {
				user, err = svc.CreateUser(context.Background(), params[0])
				Expect(err).ToNot(HaveOccurred())
				invalidRole := model.Role(0)
				update = model.UpdateUserParams{
					FullName: model.String(""),
					Email:    model.String(""),
					Role:     &invalidRole,
				}
			})

			It("returns ValidationError", func() {
				Expect(model.IsValidationError(err)).To(BeTrue())
			})
		})

		Context("when email is already in use", func() {
			BeforeEach(func() {
				var user2 *model.User
				user, err = svc.CreateUser(context.Background(), params[0])
				Expect(err).ToNot(HaveOccurred())
				user2, err = svc.CreateUser(context.Background(), params[1])
				Expect(err).ToNot(HaveOccurred())
				update = model.UpdateUserParams{Email: &user2.Email}
			})

			It("returns ErrUserEmailExists error", func() {
				Expect(err).To(MatchError(model.ErrUserEmailExists))
				Expect(model.IsValidationError(err)).To(BeTrue())
			})
		})

		Context("when user not found", func() {
			BeforeEach(func() {
				user = new(model.User)
			})

			It("returns ErrUserNotFound error", func() {
				Expect(err).To(MatchError(model.ErrUserNotFound))
				Expect(model.IsNotFoundError(err)).To(BeTrue())
			})
		})
	})

	Describe("user delete", func() {
		var (
			params = testCreateUserParams()[0]
			user   *model.User
			err    error
		)

		JustBeforeEach(func() {
			err = svc.DeleteUserByID(context.Background(), user.ID)
		})

		Context("when existing user deleted", func() {
			BeforeEach(func() {
				user, err = svc.CreateUser(context.Background(), params)
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("removes user from the database", func() {
				_, err = svc.FindUserByID(context.Background(), user.ID)
				Expect(err).To(MatchError(model.ErrUserNotFound))
			})

			It("allows user with the same email to be created", func() {
				user, err = svc.CreateUser(context.Background(), params)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when non-existing user deleted", func() {
			BeforeEach(func() {
				user = new(model.User)
			})

			It("does not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})

func testCreateUserParams() []model.CreateUserParams {
	return []model.CreateUserParams{
		{
			FullName: model.String("John Doe"),
			Email:    "john@example.com",
			Role:     model.ViewerRole,
			Password: []byte("qwerty"),
		},
		{
			FullName: model.String("admin"),
			Email:    "admin@local.domain",
			Role:     model.AdminRole,
			Password: []byte("qwerty"),
		},
	}
}

func expectUserMatches(user *model.User, params model.CreateUserParams) {
	Expect(user.FullName).To(Equal(*params.FullName))
	Expect(user.Email).To(Equal(params.Email))
	Expect(user.Role).To(Equal(params.Role))
	Expect(user.CreatedAt).ToNot(BeZero())
	Expect(user.UpdatedAt).ToNot(BeZero())
	Expect(user.DeletedAt).To(BeZero())
}
